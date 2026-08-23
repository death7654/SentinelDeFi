<#
SentinelDeFi — full pipeline bootstrap (Windows / PowerShell)

Run order:
  1. docker-compose up -d          (Zookeeper + Kafka + Grafana)
  2. Wait for the Kafka broker to accept connections
  3. Create the defi-transactions topic (6 partitions, idempotent)
  4. Generate synthetic wallet profiles + stage the CSV where
     storage_layer.py expects it (C:\sentineldefi\wallet_profiles.csv)
  5. Provision the Hive database + Delta Lake anomaly table
  6. Sanity-check the broadcast hash join
  7. Launch metrics_api.py (FastAPI/uvicorn) in its own window — this MUST
     be up before streaming_engine.py starts pushing batch metrics to it,
     and it's what Grafana polls for the live dashboard
  8. Launch transaction_generator.py in its own window
  9. Launch streaming_engine.py in its own window

NOTE: this script does not train the KMeans model. Run `python
train_kmeans.py` once beforehand (see README Step 4) — streaming_engine.py
runs fine without it, but falls back to rule-based detection only.

Usage (from the project root, in PowerShell):
    powershell -ExecutionPolicy Bypass -File launch.ps1
#>

# Deliberately NOT setting $ErrorActionPreference = "Stop" globally. Native
# tools (java, docker, python) routinely write benign, expected output to
# stderr — e.g. `java -version` prints its version string to stderr, not
# stdout, even on a clean exit 0. With $ErrorActionPreference = "Stop",
# PowerShell (both Windows PowerShell 5.1 and, unless
# $PSNativeCommandUseErrorActionPreference is disabled, PowerShell 7.3+)
# promotes that stderr text into a terminating exception regardless of
# exit code, which would kill this script on the very first prerequisite
# check. Instead, every native command below is followed by an explicit
# $LASTEXITCODE check, which is what actually reflects success/failure.

$ProjectRoot = $PSScriptRoot
$SentinelDataDir = "C:\sentineldefi"

function Write-Step($msg) {
    Write-Host "`n==> $msg" -ForegroundColor Cyan
}

# ---------------------------------------------------------------------------
# 0. Pre-flight checks
# ---------------------------------------------------------------------------
Write-Step "Checking prerequisites"

java -version 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: 'java' not found on PATH. JDK 17 or 21 is required for PySpark." -ForegroundColor Yellow
}

if (-not (Test-Path "C:\hadoop\bin\winutils.exe")) {
    Write-Host "WARNING: C:\hadoop\bin\winutils.exe not found. PySpark will fail without it (see README troubleshooting)." -ForegroundColor Yellow
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker not found on PATH. Install Docker Desktop before running this script."
}

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "python not found on PATH. Activate your virtual environment first."
}

# Verify the Python packages the pipeline actually imports. requirements.txt
# is the source of truth for versions — this just fails fast here, before
# spawning windows, if `pip install -r requirements.txt` was never run.
# ("kafka" is the import name for the kafka-python package; "fastapi" /
# "uvicorn" back metrics_api.py, which Grafana's live dashboard depends on.)
$requiredPackages = @("pyspark", "kafka", "numpy", "requests", "fastapi", "uvicorn")
$missingPackages = @()
foreach ($pkg in $requiredPackages) {
    python -c "import $pkg" 2>$null
    if ($LASTEXITCODE -ne 0) {
        $missingPackages += $pkg
    }
}
if ($missingPackages.Count -gt 0) {
    Write-Host "WARNING: missing Python packages: $($missingPackages -join ', ')" -ForegroundColor Yellow
    Write-Host "         Install with: pip install -r requirements.txt" -ForegroundColor Yellow
}

# PySpark launches its JVM via JAVA_HOME, not just PATH — so even though
# `java` already resolves correctly on PATH, PySpark can still fail to find
# a JVM if JAVA_HOME is unset or points somewhere stale. Only set it if it's
# missing or doesn't actually contain a java.exe, and derive it from
# wherever `java` on PATH actually lives instead of hardcoding a guessed
# install path (Oracle, Temurin/Adoptium, Zulu, etc. all install to
# different locations, e.g. "C:\Program Files\Eclipse Adoptium\jdk-17...").
$javaHomeValid = $env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME "bin\java.exe"))
if (-not $javaHomeValid) {
    $javaCmd = Get-Command java -ErrorAction SilentlyContinue
    if ($javaCmd) {
        # java.exe -> ...\<jdk-root>\bin\java.exe
        $detectedJavaHome = Split-Path (Split-Path $javaCmd.Source -Parent) -Parent
        $env:JAVA_HOME = $detectedJavaHome
        Write-Host "  Detected JAVA_HOME: $env:JAVA_HOME" -ForegroundColor DarkGray
    } else {
        Write-Host "WARNING: could not detect JAVA_HOME (java not found on PATH). PySpark will likely fail." -ForegroundColor Yellow
    }
}
if ($env:JAVA_HOME -and ($env:PATH -notlike "*$env:JAVA_HOME\bin*")) {
    $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
}

# ---------------------------------------------------------------------------
# 1. Start Kafka + Zookeeper
# ---------------------------------------------------------------------------
Write-Step "Starting Zookeeper + Kafka (docker-compose up -d)"
docker-compose -f "$ProjectRoot\docker-compose.yml" up -d
if ($LASTEXITCODE -ne 0) {
    throw "docker-compose up -d failed (exit $LASTEXITCODE). Check Docker Desktop is running."
}

# ---------------------------------------------------------------------------
# 2. Wait for Kafka to accept connections
# ---------------------------------------------------------------------------
Write-Step "Waiting for Kafka broker to become ready"
$maxRetries = 20
$ready = $false
for ($i = 0; $i -lt $maxRetries; $i++) {
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list *> $null
    if ($LASTEXITCODE -eq 0) {
        $ready = $true
        break
    }
    Write-Host "  Kafka not ready yet, retrying in 3s... ($($i + 1)/$maxRetries)"
    Start-Sleep -Seconds 3
}
if (-not $ready) {
    throw "Kafka did not become ready in time. Check 'docker logs kafka'."
}

# ---------------------------------------------------------------------------
# 3. Create the topic (idempotent)
# ---------------------------------------------------------------------------
Write-Step "Creating defi-transactions topic (6 partitions)"
docker exec kafka kafka-topics --create --if-not-exists `
    --topic defi-transactions `
    --bootstrap-server localhost:9092 `
    --partitions 6 --replication-factor 1
if ($LASTEXITCODE -ne 0) {
    throw "Failed to create the defi-transactions topic (exit $LASTEXITCODE). Check 'docker logs kafka'."
}

docker exec kafka kafka-topics --describe --topic defi-transactions --bootstrap-server localhost:9092

# ---------------------------------------------------------------------------
# 4. Generate wallet profiles + stage them where storage_layer.py expects them
# ---------------------------------------------------------------------------
Write-Step "Generating synthetic wallet profiles"
python "$ProjectRoot\generate_wallet_profiles.py"
if ($LASTEXITCODE -ne 0) {
    throw "generate_wallet_profiles.py failed (exit $LASTEXITCODE)."
}

New-Item -ItemType Directory -Force -Path $SentinelDataDir | Out-Null
Copy-Item -Force "$ProjectRoot\wallet_profiles.csv" "$SentinelDataDir\wallet_profiles.csv"
Write-Host "  Staged wallet_profiles.csv -> $SentinelDataDir\wallet_profiles.csv"

# ---------------------------------------------------------------------------
# 5. Provision Hive database + Delta anomaly table
# ---------------------------------------------------------------------------
Write-Step "Provisioning Hive tables + Delta Lake anomaly table"
python "$ProjectRoot\storage_layer.py"
if ($LASTEXITCODE -ne 0) {
    throw "storage_layer.py failed (exit $LASTEXITCODE). Check the JDK/winutils setup in README Prerequisites."
}

# ---------------------------------------------------------------------------
# 6. Sanity-check the broadcast join
# ---------------------------------------------------------------------------
Write-Step "Verifying broadcast hash join"
python "$ProjectRoot\broadcast_engine.py"
if ($LASTEXITCODE -ne 0) {
    throw "broadcast_engine.py failed (exit $LASTEXITCODE)."
}

# ---------------------------------------------------------------------------
# 7. Launch the metrics API (FastAPI/uvicorn) in its own window
# ---------------------------------------------------------------------------
# This has to be up BEFORE streaming_engine.py starts writing micro-batches,
# since process_micro_batch() posts to http://localhost:8000/metrics/update
# on every batch. It's also what Grafana's Infinity datasource polls at
# http://host.docker.internal:8000/metrics/summary for the live dashboard
# (see README "Live Grafana Dashboard").
Write-Step "Launching metrics_api.py (FastAPI) in a new window"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; python -m uvicorn metrics_api:app --host 0.0.0.0 --port 8000"

Write-Step "Waiting for metrics API to become ready"
$apiReady = $false
for ($i = 0; $i -lt 15; $i++) {
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:8000/health" -UseBasicParsing -TimeoutSec 2
        if ($resp.StatusCode -eq 200) {
            $apiReady = $true
            break
        }
    } catch {
        # not up yet
    }
    Write-Host "  metrics_api not ready yet, retrying in 1s... ($($i + 1)/15)"
    Start-Sleep -Seconds 1
}
if (-not $apiReady) {
    Write-Host "WARNING: metrics_api.py did not respond on http://localhost:8000/health in time. Check the metrics_api window for errors." -ForegroundColor Yellow
}

# ---------------------------------------------------------------------------
# 8. Launch the producer and the streaming engine in their own windows
# ---------------------------------------------------------------------------
Write-Step "Launching transaction_generator.py in a new window"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; python transaction_generator.py"

Start-Sleep -Seconds 2

Write-Step "Launching streaming_engine.py in a new window"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; python streaming_engine.py"

Write-Host "`nPipeline started. Three new windows are now running: metrics_api, the producer, and the streaming engine." -ForegroundColor Green
Write-Host "  Metrics API:  http://localhost:8000/metrics/summary" -ForegroundColor Green
Write-Host "  Grafana:      http://localhost:3000  (admin / admin)" -ForegroundColor Green
Write-Host "  See README 'Live Grafana Dashboard' to wire up the Infinity datasource panel." -ForegroundColor Green