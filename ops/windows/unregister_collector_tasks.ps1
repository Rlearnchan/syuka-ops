param(
    [string]$TaskPrefix = "syuka-ops"
)

$ErrorActionPreference = "Stop"

$taskNames = @(
    "$TaskPrefix collector incremental 0900",
    "$TaskPrefix collector incremental 1400",
    "$TaskPrefix collector retry-failed 1420"
)

foreach ($taskName in $taskNames) {
    try {
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction Stop
        Write-Host "삭제 완료: $taskName"
    } catch {
        Write-Host "건너뜀: $taskName"
    }
}
