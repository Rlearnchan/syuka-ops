param(
    [string]$TaskPrefix = "syuka-ops",
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
)

$ErrorActionPreference = "Stop"

$runIncremental = Join-Path $ProjectRoot "ops\windows\run_incremental.bat"
$retryFailed = Join-Path $ProjectRoot "ops\windows\retry_failed.bat"

foreach ($path in @($runIncremental, $retryFailed)) {
    if (-not (Test-Path $path)) {
        throw "필수 파일이 없습니다: $path"
    }
}

$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $currentUser -LogonType InteractiveToken -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4)

$incrementalAction = New-ScheduledTaskAction -Execute $runIncremental
$retryAction = New-ScheduledTaskAction -Execute $retryFailed

$incrementalMorning = New-ScheduledTaskTrigger -Daily -At 9:00AM
$incrementalAfternoon = New-ScheduledTaskTrigger -Daily -At 2:00PM
$retryAfternoon = New-ScheduledTaskTrigger -Daily -At 2:20PM

Register-ScheduledTask `
    -TaskName "$TaskPrefix collector incremental 0900" `
    -Action $incrementalAction `
    -Trigger $incrementalMorning `
    -Principal $principal `
    -Settings $settings `
    -Description "syuka-ops collector incremental run at 09:00" `
    -Force | Out-Null

Register-ScheduledTask `
    -TaskName "$TaskPrefix collector incremental 1400" `
    -Action $incrementalAction `
    -Trigger $incrementalAfternoon `
    -Principal $principal `
    -Settings $settings `
    -Description "syuka-ops collector incremental run at 14:00" `
    -Force | Out-Null

Register-ScheduledTask `
    -TaskName "$TaskPrefix collector retry-failed 1420" `
    -Action $retryAction `
    -Trigger $retryAfternoon `
    -Principal $principal `
    -Settings $settings `
    -Description "syuka-ops collector retry-failed run at 14:20" `
    -Force | Out-Null

Write-Host "등록 완료:"
Write-Host "- $TaskPrefix collector incremental 0900"
Write-Host "- $TaskPrefix collector incremental 1400"
Write-Host "- $TaskPrefix collector retry-failed 1420"
