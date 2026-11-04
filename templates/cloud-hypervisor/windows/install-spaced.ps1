# Install the native service and optionally provision a regular process/file account.
# This does not qualify VM memory lifecycle.
[CmdletBinding()]
param([Parameter(Mandatory = $true)][string]$Executable, [switch]$EnableOperations)
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
if ($EnableOperations) {
    $credentialPath = Join-Path $directory 'identity.json'
    if (-not (Test-Path -LiteralPath $credentialPath)) {
        if (Get-LocalUser -Name user -ErrorAction SilentlyContinue) {
            throw 'Refusing to take over an existing user account without SPACE identity evidence.'
        }
        $random = [byte[]]::new(32)
        $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
        try { $rng.GetBytes($random) } finally { $rng.Dispose() }
        $password = [Convert]::ToBase64String($random) + 'aA1!'
        $secure = ConvertTo-SecureString $password -AsPlainText -Force
        $createdUser = New-LocalUser -Name user -Password $secure -AccountNeverExpires -PasswordNeverExpires -UserMayNotChangePassword -Description 'SPACE session-0 process account'
        try {
            $users = Get-LocalGroup -SID 'S-1-5-32-545'
            Add-LocalGroupMember -Group $users -Member $createdUser
            # This directory is already restricted to SYSTEM and Administrators.
            # Never print this private per-image credential or include it in source.
            $json = @{ user = 'user'; password = $password } | ConvertTo-Json -Compress
            [IO.File]::WriteAllText($credentialPath, $json, [Text.UTF8Encoding]::new($false))
        } catch {
            Remove-Item -LiteralPath $credentialPath -Force -ErrorAction SilentlyContinue
            Remove-LocalUser -SID $createdUser.SID -ErrorAction SilentlyContinue
            throw
        } finally {
            $password = $null; $json = $null
            $secure.Dispose(); $secure = $null
            [Array]::Clear($random, 0, $random.Length)
        }
    }
}
$existing = Get-CimInstance Win32_Service -Filter "Name='spaced'"
if ($existing -and $existing.PathName.Trim('"') -ne $target) {
    throw 'An unrelated spaced service already exists; refusing to replace it.'
}
if ($existing) { Stop-Service spaced }
# Unlink an old destination instead of writing through a possible hard link.
if (Test-Path -LiteralPath $target) { Remove-Item -LiteralPath $target -Force }
Copy-Item -LiteralPath $source -Destination $target -Force
# The privileged service launches tenant work only with the provisioned regular account.
if (-not $existing) {
    New-Service -Name spaced -BinaryPathName ('"' + $target + '"') -StartupType Automatic -DependsOn VirtioSocket | Out-Null
} else {
    sc.exe config spaced start= auto depend= VirtioSocket | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not configure the spaced service.' }
}
# Recover an unexpected listener/process failure without restarting a healthy service.
sc.exe failure spaced reset= 86400 actions= restart/5000/restart/30000/restart/60000 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Could not configure spaced failure recovery.' }
sc.exe failureflag spaced 1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Could not enable spaced failure recovery.' }
Start-Service spaced
Get-Service spaced
