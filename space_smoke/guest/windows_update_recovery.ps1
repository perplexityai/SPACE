# Fault injection is restricted to the scenario's newly created disposable VM.
# Stage identical bytes under a synthetic journal attempt: this qualifies local
# recovery independently of manager release authorization or fleet policy.
$ErrorActionPreference = 'Stop'
$root = 'C:\ProgramData\SPACE\agent'
Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class GuestRecoveryFault {
    [StructLayout(LayoutKind.Sequential)] public struct Status {
        public uint Type, State, Controls, ExitCode, ServiceExitCode, Checkpoint, WaitHint, ProcessId, Flags;
    }
    [DllImport("advapi32.dll", CharSet=CharSet.Unicode, SetLastError=true)] static extern IntPtr OpenSCManager(string machine, string database, uint access);
    [DllImport("advapi32.dll", CharSet=CharSet.Unicode, SetLastError=true)] static extern IntPtr OpenService(IntPtr manager, string name, uint access);
    [DllImport("advapi32.dll", SetLastError=true)] static extern bool QueryServiceStatusEx(IntPtr service, int level, out Status status, int size, out int needed);
    [DllImport("advapi32.dll")] static extern bool CloseServiceHandle(IntPtr handle);
    [DllImport("kernel32.dll", SetLastError=true)] public static extern IntPtr OpenProcess(uint access, bool inherit, uint pid);
    [DllImport("kernel32.dll")] public static extern bool CloseHandle(IntPtr handle);
    [DllImport("kernel32.dll")] public static extern ulong GetTickCount64();
    [DllImport("ntdll.dll")] public static extern int NtSuspendProcess(IntPtr handle);
    public static Status Service() {
        IntPtr manager = OpenSCManager(null, null, 1);
        if (manager == IntPtr.Zero) throw new System.ComponentModel.Win32Exception();
        try {
            IntPtr service = OpenService(manager, "spaced", 4);
            if (service == IntPtr.Zero) throw new System.ComponentModel.Win32Exception();
            try {
                Status status; int needed;
                if (!QueryServiceStatusEx(service, 0, out status, Marshal.SizeOf(typeof(Status)), out needed))
                    throw new System.ComponentModel.Win32Exception();
                return status;
            } finally { CloseServiceHandle(service); }
        } finally { CloseServiceHandle(manager); }
    }
}
'@
function Journal { Get-Content -LiteralPath "$root\update-state" -Raw | ConvertFrom-Json }
# Query SCM directly: optional WMI providers can be absent or stalled in a seed.
function Service { [GuestRecoveryFault]::Service() }
function Await($Condition, [int]$Seconds) {
    $timer = [Diagnostics.Stopwatch]::StartNew()
    while (-not (& $Condition)) {
        if ($timer.Elapsed.TotalSeconds -ge $Seconds) { throw 'Recovery condition timed out' }
        Start-Sleep -Milliseconds 100
    }
}
& schtasks.exe /Change /TN SPACE-Guest-Expiry /Disable | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Cannot disable expiry during fault setup' }
try {
    Stop-Service spaced
    Await { (Service).State -eq 1 } 30
    # No update may already be active on this fresh fixture.
    $lock = [IO.File]::Open("$root\update-lock", 'Open', 'ReadWrite', 'ReadWrite')
    try {
        Await {
            try { $lock.Lock(0, 1); return $true }
            catch [IO.IOException] {
                if (($_.Exception.HResult -band 0xffff) -notin @(32, 33)) { throw }
                return $false
            }
        } 5
        $journal = Journal
        if ($journal.state.phase -notin @('stable', 'committed', 'restarted')) { throw 'Fixture is not settled' }
        $committed = $journal.committed
        $other = if ($committed.slot -eq 'a') { 'b' } else { 'a' }
        foreach ($name in @('spaced-svc.exe', 'spaced.exe', 'artifact.json')) {
            Copy-Item -LiteralPath "$root\slots\$($committed.slot)\$name" -Destination "$root\slots\$other\$name" -Force
        }
        $attempt = @{
            operation = [guid]::NewGuid().ToString(); execution = [guid]::NewGuid().ToString()
            incarnation = [guid]::NewGuid().ToString()
            boot = (Get-ItemProperty 'HKLM:\SOFTWARE\SPACE\GuestUpdateBoot').Epoch
            configuration_revision = 1
            target = @{ slot = $other; digest = $committed.digest }
            confirmation_deadline_ms = [GuestRecoveryFault]::GetTickCount64() + 120000
        }
        $journal.state = @{ phase = 'armed'; attempt = $attempt }
        [IO.File]::WriteAllText("$root\update-state", ($journal | ConvertTo-Json -Depth 10 -Compress), [Text.UTF8Encoding]::new($false))
    } finally { $lock.Dispose() }
    & schtasks.exe /Run /TN SPACE-Guest-Recovery | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Cannot request trial start' }
    Await { (Journal).state.phase -eq 'trial' -and (Service).ProcessId -ne 0 } 30
    $candidate = (Service).ProcessId
    $handle = [GuestRecoveryFault]::OpenProcess(0x0800, $false, $candidate)
    if ($handle -eq [IntPtr]::Zero) { throw 'Cannot pin candidate for suspension' }
    try {
        if ([GuestRecoveryFault]::NtSuspendProcess($handle) -ne 0) { throw 'Cannot suspend candidate' }
    } finally { [GuestRecoveryFault]::CloseHandle($handle) | Out-Null }
    if ((Journal).state.phase -ne 'trial') { throw 'Candidate settled before the hang' }
    & schtasks.exe /Change /TN SPACE-Guest-Expiry /Enable | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Cannot enable expiry' }
    # No manual recovery kick: the installed deadline/task must rescue the hang.
    Await { (Journal).state.phase -eq 'rolled_back' -and (Service).State -eq 4 } 180
    $recovered = Service
    if ($recovered.ProcessId -eq $candidate -or (Get-Process -Id $candidate -ErrorAction SilentlyContinue)) {
        throw 'Hung candidate survived recovery'
    }
    if ((Journal).committed.digest -ne $committed.digest -or [IO.File]::ReadAllText("$root\current") -ne $committed.slot) {
        throw 'Recovery did not restore committed selection'
    }
    # A settled journal must still have a retry owner if start or recovery failed.
    Stop-Service spaced
    Await { (Service).State -eq 1 } 30
    Await { (Service).State -eq 4 } 120
    if ((Journal).committed.digest -ne $committed.digest) { throw 'Stopped-service retry changed release' }
    @{ outcome = 'recovered'; committed_digest = $committed.digest; killed_pid = $candidate } | ConvertTo-Json -Compress
} finally {
    & schtasks.exe /Change /TN SPACE-Guest-Expiry /Enable | Out-Null
}
