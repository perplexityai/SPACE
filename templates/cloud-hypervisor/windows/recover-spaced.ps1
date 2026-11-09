# Permanent image ABI: tasks select a committed executable, never a third copy.
[CmdletBinding()]
param([switch]$ExpiredOnly)
$ErrorActionPreference = 'Stop'
$root = 'C:\ProgramData\SPACE\agent'
$lock = $null
$pin = $null
try {
    $lock = [IO.File]::Open((Join-Path $root 'update-lock'), 'Open', 'ReadWrite', 'ReadWrite')
    # Overlaps Rust's whole-file lock. Pin the chosen executable before releasing
    # it, so concurrent staging cannot replace that slot between read and exec.
    $lock.Lock(0, 1)
    try {
        $slot = [IO.File]::ReadAllText((Join-Path $root 'committed'))
        if ($slot -cnotin @('a', 'b')) { throw 'Invalid committed selector.' }
        $executable = Join-Path $root ('slots\' + $slot + '\spaced-svc.exe')
        $pin = [IO.File]::Open($executable, 'Open', 'Read', 'Read')
    } finally {
        $lock.Unlock(0, 1)
        $lock.Dispose()
        $lock = $null
    }
    $arguments = @('--recover-update')
    if ($ExpiredOnly) { $arguments += '--expired-only' }
    & $executable @arguments
    if ($LASTEXITCODE -ne 0) { throw "Guest recovery failed ($LASTEXITCODE)." }
} finally {
    if ($pin) { $pin.Dispose() }
    if ($lock) { $lock.Dispose() }
}
