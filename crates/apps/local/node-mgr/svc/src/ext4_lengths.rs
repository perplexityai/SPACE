use std::collections::HashSet;
use std::ffi::CString;
use std::fs::{File, FileTimes};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::process::Command;

pub(crate) fn restore_file_lengths(image: &Path, destination: &Path) -> io::Result<()> {
    let stats = Command::new("debugfs")
        .env("LC_ALL", "C")
        .args(["-R", "stats"])
        .arg(image)
        .output()?;
    let stats_text = String::from_utf8_lossy(&stats.stdout);
    let features = stats_text
        .lines()
        .find_map(|line| line.strip_prefix("Filesystem features:"))
        .ok_or_else(invalid_listing)?;
    if !features
        .split_whitespace()
        .any(|feature| feature == "inline_data")
    {
        return Ok(());
    }
    let root = File::options()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(destination)?;
    let mut pending = vec![(2_u64, Vec::<Vec<u8>>::new())];
    let mut visited = HashSet::new();
    while let Some((inode, components)) = pending.pop() {
        let mut directory = root.try_clone()?;
        for component in &components {
            directory = open_child(&directory, component, libc::O_RDONLY | libc::O_DIRECTORY)?;
        }
        if !visited.insert(inode) {
            return Err(invalid_listing());
        }
        let output = Command::new("debugfs")
            .env("LC_ALL", "C")
            .args(["-R", &format!("ls -p <{inode}>")])
            .arg(image)
            .output()?;
        if !output.status.success() {
            return Err(io::Error::other("debugfs directory listing failed"));
        }
        let entries = parse_listing(&output.stdout)?;
        for entry in entries {
            match entry.mode & libc::S_IFMT {
                libc::S_IFDIR => {
                    let mut child = components.clone();
                    child.push(entry.name.to_vec());
                    pending.push((entry.inode, child));
                }
                libc::S_IFREG => {
                    let mut file =
                        open_child(&directory, entry.name, libc::O_RDONLY | libc::O_NONBLOCK)?;
                    let metadata = file.metadata()?;
                    if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() < entry.size {
                        return Err(io::Error::other(
                            "extracted file does not match its ext4 inode",
                        ));
                    }
                    if metadata.len() != entry.size {
                        let stat = Command::new("debugfs")
                            .env("LC_ALL", "C")
                            .args(["-R", &format!("stat <{}>", entry.inode)])
                            .arg(image)
                            .output()?;
                        let stat_text = String::from_utf8_lossy(&stat.stdout);
                        let flags = stat_text
                            .lines()
                            .next()
                            .and_then(|line| line.split_once("Flags: 0x"))
                            .and_then(|(_, flags)| u32::from_str_radix(flags.trim(), 16).ok())
                            .ok_or_else(invalid_listing)?;
                        if flags & 0x1000_0000 == 0 {
                            return Err(io::Error::other(
                                "non-inline ext4 extraction length mismatch",
                            ));
                        }
                        let extra = metadata.len() - entry.size;
                        if extra > 4096 {
                            return Err(io::Error::other("unexpected ext4 extraction length"));
                        }
                        file.seek(SeekFrom::Start(entry.size))?;
                        let mut padding = Vec::new();
                        file.by_ref().take(extra + 1).read_to_end(&mut padding)?;
                        if padding.len() as u64 != extra || padding.iter().any(|byte| *byte != 0) {
                            return Err(io::Error::other("unexpected ext4 extraction padding"));
                        }
                        let writer =
                            open_child(&directory, entry.name, libc::O_WRONLY | libc::O_NONBLOCK)?;
                        let current = writer.metadata()?;
                        if current.dev() != metadata.dev()
                            || current.ino() != metadata.ino()
                            || current.len() != metadata.len()
                        {
                            return Err(io::Error::other(
                                "extracted file changed during validation",
                            ));
                        }
                        writer.set_len(entry.size)?;
                        writer.set_permissions(metadata.permissions())?;
                        writer.set_times(
                            FileTimes::new()
                                .set_accessed(metadata.accessed()?)
                                .set_modified(metadata.modified()?),
                        )?;
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

struct Entry<'a> {
    inode: u64,
    mode: libc::mode_t,
    name: &'a [u8],
    size: u64,
}

fn invalid_listing() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid ext4 directory listing")
}

fn number(bytes: &[u8], radix: u32) -> io::Result<u64> {
    let value = std::str::from_utf8(bytes).map_err(|_| invalid_listing())?;
    u64::from_str_radix(value, radix).map_err(|_| invalid_listing())
}

fn parse_listing(mut input: &[u8]) -> io::Result<Vec<Entry<'_>>> {
    let mut entries = Vec::new();
    let mut saw_directory = false;
    // The parseable format uses '/' delimiters; valid filenames can contain
    // newlines and non-UTF8 bytes, but never '/'. Do not split into lines.
    while let Some(rest) = input.strip_prefix(b"/") {
        input = rest;
        let mut fields = [&[][..]; 6];
        for field in &mut fields {
            let end = input
                .iter()
                .position(|byte| *byte == b'/')
                .ok_or_else(invalid_listing)?;
            *field = &input[..end];
            input = &input[end + 1..];
        }
        input = input.strip_prefix(b"\n").ok_or_else(invalid_listing)?;
        let inode = number(fields[0], 10)?;
        let mode = libc::mode_t::try_from(number(fields[1], 8)?).map_err(|_| invalid_listing())?;
        let name = fields[4];
        if name == b"." && mode & libc::S_IFMT == libc::S_IFDIR {
            saw_directory = true;
        }
        if inode == 0 || name == b"." || name == b".." {
            continue;
        }
        if name.is_empty() || name.contains(&0) {
            return Err(invalid_listing());
        }
        let size = if mode & libc::S_IFMT == libc::S_IFDIR {
            0
        } else {
            number(fields[5], 10)?
        };
        entries.push(Entry {
            inode,
            mode,
            name,
            size,
        });
    }
    if !saw_directory || input.iter().any(|byte| *byte != b'\n') {
        return Err(invalid_listing());
    }
    Ok(entries)
}

fn open_child(parent: &File, name: &[u8], flags: libc::c_int) -> io::Result<File> {
    let name = CString::new(name).map_err(|_| invalid_listing())?;
    // Every component is opened relative to an already held directory, and
    // O_NOFOLLOW rejects replacement symlinks at both directory and file leaves.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(all(test, target_os = "linux"))]
#[path = "ext4_lengths_tests.rs"]
mod tests;
