
param(
    [int]$IntervalSeconds = 60,
    [int]$TimeoutMinutes = 30,
    [string]$Database = "events_db",
    [string]$DbUser = "postgres"
)

$ErrorActionPreference = "Stop"

# --------------------------------------------------
# 1. Validate configuration
# --------------------------------------------------

$RunId = $env:E2E_RUN_ID

if ([string]::IsNullOrWhiteSpace($RunId)) {
    throw "E2E_RUN_ID is not set in this PowerShell session."
}

if ($RunId -notmatch '^[A-Za-z0-9_-]+$') {
    throw "E2E_RUN_ID contains unsupported characters: $RunId"
}

if ([string]::IsNullOrWhiteSpace($env:DB_PASSWORD)) {
    throw "DB_PASSWORD is not set in this PowerShell session."
}

if ($IntervalSeconds -lt 1 -or $TimeoutMinutes -lt 1) {
    throw "IntervalSeconds and TimeoutMinutes must be positive."
}

if (-not (Get-Command psql -ErrorAction SilentlyContinue)) {
    throw "psql was not found. Add PostgreSQL's bin directory to PATH."
}

# Prevent psql from prompting for the password.
$env:PGPASSWORD = $env:DB_PASSWORD

$Deadline = (Get-Date).AddMinutes($TimeoutMinutes)
$Previous = $null
$FoundEvents = $false

$LogPath = Join-Path $PSScriptRoot "e2e-monitor-$RunId.csv"

Write-Host ""
Write-Host "=========================================="
Write-Host " Distributed Event Platform Monitor"
Write-Host "=========================================="
Write-Host "Run ID:       $RunId"
Write-Host "Database:     $Database"
Write-Host "Poll interval: $IntervalSeconds seconds"
Write-Host "Timeout:      $TimeoutMinutes minutes"
Write-Host "CSV output:   $LogPath"
Write-Host "=========================================="
Write-Host ""

# --------------------------------------------------
# 2. Poll PostgreSQL
# --------------------------------------------------

while ((Get-Date) -lt $Deadline) {

    $sql = @"
SELECT
    (SELECT COUNT(*) FROM ingestion_outbox
     WHERE event_id LIKE 'e2e-$RunId-%'
       AND status = 'PENDING'),
    (SELECT COUNT(*) FROM ingestion_outbox
     WHERE event_id LIKE 'e2e-$RunId-%'
       AND status = 'PUBLISHED'),
    (SELECT COUNT(*) FROM events
     WHERE event_id LIKE 'e2e-$RunId-%'),
    (SELECT COUNT(*) FROM analytics_processed_events
     WHERE event_id LIKE 'e2e-$RunId-%');
"@

    try {
        $output = & psql `
            -X `
            -q `
            -A `
            -t `
            -w `
            -F "," `
            -v ON_ERROR_STOP=1 `
            -U $DbUser `
            -d $Database `
            -c $sql 2>&1

        if ($LASTEXITCODE -ne 0) {
            throw "PostgreSQL query failed: $($output -join ' ')"
        }

        $line = ($output | Out-String).Trim()
        $values = $line.Split(",")

        if ($values.Count -ne 4) {
            throw "Unexpected SQL output: $line"
        }

        $pending   = [long]$values[0]
        $published = [long]$values[1]
        $processed = [long]$values[2]
        $analytics = [long]$values[3]

        $now = Get-Date

        # --------------------------------------------------
        # 3. Calculate throughput since the previous snapshot
        # --------------------------------------------------

        $publishRate = $null
        $processRate = $null
        $analyticsRate = $null

        if ($null -ne $Previous) {
            $elapsed = ($now - $Previous.Time).TotalSeconds

            if ($elapsed -gt 0) {
                $publishRate = [math]::Round(
                        ($published - $Previous.Published) / $elapsed, 2
                )

                $processRate = [math]::Round(
                        ($processed - $Previous.Processed) / $elapsed, 2
                )

                $analyticsRate = [math]::Round(
                        ($analytics - $Previous.Analytics) / $elapsed, 2
                )
            }
        }

        # --------------------------------------------------
        # 4. Display current progress
        # --------------------------------------------------

        $row = [PSCustomObject]@{
            Timestamp       = $now.ToString("yyyy-MM-dd HH:mm:ss")
            RunId           = $RunId
            Pending         = $pending
            Published       = $published
            Processed       = $processed
            Analytics       = $analytics
            PublishPerSec   = $publishRate
            ProcessPerSec   = $processRate
            AnalyticsPerSec = $analyticsRate
        }

        $row | Format-Table -AutoSize

        # --------------------------------------------------
        # 5. Append the snapshot to CSV
        # --------------------------------------------------

        if (Test-Path $LogPath) {
            $row | Export-Csv `
                -Path $LogPath `
                -NoTypeInformation `
                -Append
        }
        else {
            $row | Export-Csv `
                -Path $LogPath `
                -NoTypeInformation
        }

        # Record whether any events have been observed.
        if (($pending + $published + $processed + $analytics) -gt 0) {
            $FoundEvents = $true
        }

        $Previous = @{
            Time      = $now
            Published = $published
            Processed = $processed
            Analytics = $analytics
        }

        # --------------------------------------------------
        # 6. Stop only after a real run has drained
        # --------------------------------------------------

        if ($FoundEvents -and
                $pending -eq 0 -and
                $published -eq $processed -and
                $published -eq $analytics) {

            Write-Host ""
            Write-Host "Pipeline caught up for run $RunId."
            Write-Host "Measurements saved to: $LogPath"

            exit 0
        }

        if (-not $FoundEvents) {
            Write-Warning "No matching events observed yet for run $RunId."
        }
    }
    catch {
        Write-Warning "Monitoring error: $_"
    }

    # --------------------------------------------------
    # 7. Wait before the next snapshot
    # --------------------------------------------------

    $remaining = ($Deadline - (Get-Date)).TotalSeconds

    if ($remaining -gt 0) {
        Start-Sleep -Seconds ([math]::Min($IntervalSeconds, $remaining))
    }
}

Write-Host ""

if (-not $FoundEvents) {
    Write-Error "No matching events were found for run $RunId. Verify the run ID and event ID format."
    exit 1
}

Write-Warning "Monitoring timed out before the pipeline was confirmed drained."
Write-Host "Review the CSV file: $LogPath"
exit 1
