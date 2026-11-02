# Install the native readiness service in a privately prepared evaluation image.
# Tenant process/file operations and memory lifecycle are not qualified by this script.
[CmdletBinding()]
param([Parameter(Mandatory = $true)][string]$Executable)
$ErrorActionPreference = 'Stop'
$source = (Resolve-Path -LiteralPath $Executable).Path
$directory = Join-Path $env:ProgramData 'SPACE'
$target = Join-Path $directory 'spaced-svc.exe'
if ([string]::Equals($source, $target, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Supply an executable outside the installed service directory.'
}
# This executable runs as SYSTEM. Never update it through a tenant-writable
# directory or follow a reparse point left by another account.
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Run image preparation as an administrator.'
}
if (Test-Path -LiteralPath $directory) {
    $item = Get-Item -LiteralPath $directory -Force
    if (-not $item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        throw 'Refusing a non-directory or reparse-point service directory.'
    }
    $owner = (Get-Acl -LiteralPath $directory).GetOwner([Security.Principal.SecurityIdentifier]).Value
    if ($owner -notin @('S-1-5-18', 'S-1-5-32-544', $identity.User.Value)) {
        throw 'Refusing a service directory owned by another account.'
    }
} else {
    New-Item -ItemType Directory -Path $directory | Out-Null
}
$acl = [Security.AccessControl.DirectorySecurity]::new()
$acl.SetAccessRuleProtection($true, $false)
foreach ($sid in @('S-1-5-18', 'S-1-5-32-544')) {
    $rule = [Security.AccessControl.FileSystemAccessRule]::new(
        [Security.Principal.SecurityIdentifier]::new($sid), 'FullControl',
        'ContainerInherit,ObjectInherit', 'None', 'Allow')
    $acl.AddAccessRule($rule)
}
Set-Acl -LiteralPath $directory -AclObject $acl
$existing = Get-CimInstance Win32_Service -Filter "Name='spaced'"
if ($existing -and $existing.PathName.Trim('"') -ne $target) {
    throw 'An unrelated spaced service already exists; refusing to replace it.'
}
if ($existing) { Stop-Service spaced }
# Unlink an old destination instead of writing through a possible hard link.
if (Test-Path -LiteralPath $target) { Remove-Item -LiteralPath $target -Force }
Copy-Item -LiteralPath $source -Destination $target -Force
# The privileged service exposes readiness only. It does not run tenant commands.
if (-not $existing) {
    New-Service -Name spaced -BinaryPathName ('"' + $target + '"') -StartupType Automatic -DependsOn VirtioSocket | Out-Null
} else {
    sc.exe config spaced start= auto depend= VirtioSocket | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not configure the spaced service.' }
}
Start-Service spaced
Get-Service spaced
