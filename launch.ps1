<#
SentinelDeFi — full pipeline bootstrap (Windows / PowerShell)

Run order:
  1. docker-compose up -d          (Zookeeper + Kafka)
  2. Wait for the Kafka broker to accept connections
  3. Create the defi-transactions topic (6 partitions, idempotent)
  4. Generate synthetic wallet profiles + stage the CSV where
     storage_layer.py expects it (C:\sentineldefi\wallet_profiles.csv)
  5. Provision the Hive database + Delta Lake anomaly table
  6. Sanity-check the broadcast hash join
  7. Launch transaction_generator.py in its own window
  8. Launch streaming_engine.py in its own window

Usage (from the project root, in PowerShell):
    powershell -ExecutionPolicy Bypass -File launch.ps1
#>

$ErrorActionPreference = "Stop"

# PowerShell 7.3+ can convert ANY stderr output from a native command (java,
# python, etc.) into a terminating error when $ErrorActionPreference = "Stop",
# even on a clean 0 exit (java prints its version to stderr) or on an
# expected failure we want to handle ourselves (python's ModuleNotFoundError
# traceback below). Force the old, non-escalating behavior so we can check
# $LASTEXITCODE ourselves instead of the whole script dying on those lines.
if (Test-Path Variable:\PSNativeCommandUseErrorActionPreference) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$ProjectRoot = $PSScriptRoot
$SentinelDataDir = "C:\sentineldefi"

function Write-Step($msg) {
    Write-Host "`n==> $msg" -ForegroundColor Cyan
}

# # ---------------------------------------------------------------------------
# # 0. Pre-flight checks
# # ---------------------------------------------------------------------------
# Write-Step "Checking prerequisites"

# java -version 2>$null | Out-Null
# if ($LASTEXITCODE -ne 0) {
#     Write-Host "WARNING: 'java' not found on PATH. JDK 17 or 21 is required for PySpark." -ForegroundColor Yellow
# }

# if (-not (Test-Path "C:\hadoop\bin\winutils.exe")) {
#     Write-Host "WARNING: C:\hadoop\bin\winutils.exe not found. PySpark will fail without it (see README troubleshooting)." -ForegroundColor Yellow
# }

# if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
#     throw "Docker not found on PATH. Install Docker Desktop before running this script."
# }

# if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
#     throw "python not found on PATH. Activate your virtual environment first."
# }

# # Verify the Python packages the pipeline actually imports. The README's
# # `pip install pyspark kafka-python` line misses delta-spark (storage_layer.py)
# # and numpy (transaction_generator.py) — check for all four so a missing
# # package fails fast here instead of mid-pipeline in a spawned window.
# $requiredPackages = @("pyspark", "kafka", "delta", "numpy")
# $missingPackages = @()
# foreach ($pkg in $requiredPackages) {
#     python -c "import $pkg" 2>$null
#     if ($LASTEXITCODE -ne 0) {
#         $missingPackages += $pkg
#     }
# }
# if ($missingPackages.Count -gt 0) {
#     Write-Host "WARNING: missing Python packages: $($missingPackages -join ', ')" -ForegroundColor Yellow
#     Write-Host "         Install with: pip install pyspark kafka-python delta-spark numpy" -ForegroundColor Yellow
# }

$env:JAVA_HOME = "C:\Program Files\Java\jdk-17\"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

# ---------------------------------------------------------------------------
# 1. Start Kafka + Zookeeper
# ---------------------------------------------------------------------------
Write-Step "Starting Zookeeper + Kafka (docker-compose up -d)"
docker-compose -f "$ProjectRoot\docker-compose.yml" up -d

# ---------------------------------------------------------------------------
# 2. Wait for Kafka to accept connections
# ---------------------------------------------------------------------------
Write-Step "Waiting for Kafka broker to become ready"
$maxRetries = 20
$ready = $false
for ($i = 0; $i -lt $maxRetries; $i++) {
    docker exec defi-kafka kafka-topics --bootstrap-server localhost:9092 --list *> $null
    if ($LASTEXITCODE -eq 0) {
        $ready = $true
        break
    }
    Write-Host "  Kafka not ready yet, retrying in 3s... ($($i + 1)/$maxRetries)"
    Start-Sleep -Seconds 3
}
if (-not $ready) {
    throw "Kafka did not become ready in time. Check 'docker logs defi-kafka'."
}

# ---------------------------------------------------------------------------
# 3. Create the topic (idempotent)
# ---------------------------------------------------------------------------
Write-Step "Creating defi-transactions topic (6 partitions)"
docker exec defi-kafka kafka-topics --create --if-not-exists `
    --topic defi-transactions `
    --bootstrap-server localhost:9092 `
    --partitions 6 --replication-factor 1

docker exec defi-kafka kafka-topics --describe --topic defi-transactions --bootstrap-server localhost:9092

# ---------------------------------------------------------------------------
# 4. Generate wallet profiles + stage them where storage_layer.py expects them
# ---------------------------------------------------------------------------
Write-Step "Generating synthetic wallet profiles"
python "$ProjectRoot\generate_wallet_profiles.py"

New-Item -ItemType Directory -Force -Path $SentinelDataDir | Out-Null
Copy-Item -Force "$ProjectRoot\wallet_profiles.csv" "$SentinelDataDir\wallet_profiles.csv"
Write-Host "  Staged wallet_profiles.csv -> $SentinelDataDir\wallet_profiles.csv"

# ---------------------------------------------------------------------------
# 5. Provision Hive database + Delta anomaly table
# ---------------------------------------------------------------------------
Write-Step "Provisioning Hive tables + Delta Lake anomaly table"
python "$ProjectRoot\storage_layer.py"

# ---------------------------------------------------------------------------
# 6. Sanity-check the broadcast join
# ---------------------------------------------------------------------------
Write-Step "Verifying broadcast hash join"
python "$ProjectRoot\broadcast_engine.py"

# ---------------------------------------------------------------------------
# 7. Launch the producer and the streaming engine in their own windows
# ---------------------------------------------------------------------------
Write-Step "Launching transaction_generator.py in a new window"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; python transaction_generator.py"

Start-Sleep -Seconds 2

Write-Step "Launching streaming_engine.py in a new window"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; python streaming_engine.py"

Write-Host "`nPipeline started. Two new windows are now running the producer and the streaming engine." -ForegroundColor Green