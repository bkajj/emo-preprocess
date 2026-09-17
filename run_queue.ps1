Set-Location $PSScriptRoot

$configs = @(
  "configs\gradient_boosting.yaml"
)

New-Item -ItemType Directory -Force -Path logs | Out-Null
$summary = "logs\summary_$(Get-Date -Format 'yyMMdd_HHmm').txt"

foreach ($cfg in $configs) {
    $name = [IO.Path]::GetFileNameWithoutExtension($cfg)

    if (-not (Test-Path $cfg)) {
        "BRAK PLIKU: $cfg" | Tee-Object -FilePath $summary -Append
        continue
    }

    $log = "logs\$($name)_$(Get-Date -Format 'yyMMdd_HHmm').log"
    "[$(Get-Date -Format 'HH:mm:ss')] START $name" | Tee-Object -FilePath $summary -Append
    $start = Get-Date

    python pipeline.py --config $cfg *> $log
    $code = $LASTEXITCODE

    $mins = [int]((Get-Date) - $start).TotalMinutes
    if ($code -eq 0) {
        "[$(Get-Date -Format 'HH:mm:ss')] OK   $name ($mins min)" | Tee-Object -FilePath $summary -Append
    } else {
        "[$(Get-Date -Format 'HH:mm:ss')] FAIL $name (kod $code, log: $log)" | Tee-Object -FilePath $summary -Append
    }
}

"Kolejka zakonczona." | Tee-Object -FilePath $summary -Append