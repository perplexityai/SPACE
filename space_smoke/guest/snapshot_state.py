"""A live process and full-byte disk oracle for suspend/restore qualification."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import socket
import sys
import time
from pathlib import Path

SOCKET = "/tmp/space-smoke-snapshot-state.sock"
DISK = Path("/tmp/space-smoke-snapshot-state.bin")
CHUNK = 1024 * 1024


def block(kind: str, generation: int, index: int) -> bytes:
    return hashlib.shake_256(f"{kind}:{generation}:{index}".encode()).digest(CHUNK)


def serve(memory_mib: int) -> None:
    total_kib = int(Path("/proc/meminfo").read_text().split("MemTotal:", 1)[1].split()[0])
    if memory_mib < 1 or memory_mib * 1024 > total_kib // 2:
        raise ValueError("working set must be positive and at most half the guest RAM")
    memory = bytearray(memory_mib * CHUNK)
    nonce = secrets.token_hex(16)
    boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    generation = -1

    def mutate() -> None:
        nonlocal generation
        generation += 1
        for index in range(memory_mib):
            memory[index * CHUNK:(index + 1) * CHUNK] = block("ram", generation, index)
        with DISK.open("wb") as disk:
            for index in range(8):
                disk.write(block("disk", generation, index))
            disk.flush()
            os.fsync(disk.fileno())

    mutate()
    with socket.socket(socket.AF_UNIX) as listener:
        listener.bind(SOCKET)
        listener.listen()
        while True:
            connection, _ = listener.accept()
            with connection:
                operation = connection.recv(32).decode()
                if operation == "mutate":
                    mutate()
                elif operation != "read":
                    raise ValueError(f"unknown operation: {operation}")
                with DISK.open("rb") as disk:
                    # Do not let file pages carried by the memory snapshot
                    # substitute for reading the restored rootfs bytes.
                    os.posix_fadvise(disk.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
                    disk_digest = hashlib.file_digest(disk, "sha256").hexdigest()
                connection.sendall(json.dumps({
                    "pid": os.getpid(), "nonce": nonce, "boot_id": boot_id,
                    "generation": generation, "memory_mib": memory_mib,
                    "memory_sha256": hashlib.sha256(memory).hexdigest(),
                    "disk_sha256": disk_digest,
                }).encode())


def request(operation: str, timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    with socket.socket(socket.AF_UNIX) as connection:
        while True:
            try:
                connection.connect(SOCKET)
                break
            except (FileNotFoundError, ConnectionRefusedError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        connection.settimeout(timeout_seconds)
        connection.sendall(operation.encode())
        output = bytearray()
        while data := connection.recv(4096):
            output.extend(data)
        print(output.decode())


if __name__ == "__main__":
    if sys.argv[1] == "serve":
        serve(int(sys.argv[2]))
    else:
        request(sys.argv[1], int(sys.argv[2]))
