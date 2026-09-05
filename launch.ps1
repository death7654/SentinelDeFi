<#
SentinelDeFi — full pipeline bootstrap (Windows / PowerShell), v2: Neo4j
instead of Hive + Delta Lake.

Run order:
  0. Load .env (NEO4J_PASSWORD etc.) into the process environment
  1. Check prerequisites (Java, winutils, Docker, Python packages)
  2. Train the Isolation Forest model if isoforest_model.joblib doesn't
     already exist (idempotent — safe to run every time)
  3. docker-compose up -d          (Zookeeper + Kafka + Neo4j + Grafana)
  4. Wait for the Kafka broker to accept connections
  5. Wait for Neo4j's Bolt port to accept connections
  6. Create the defi-transactions topic (6 partitions, idempotent)
  7. Generate synthetic wallet profiles and load them into Neo4j
  8. Sanity-check the broadcast hash join
  9. Run graph_analytics.py once (so PageRank/betweenness/community/
     embedding/wash-ring properties exist before the first streaming
     batch reads them) and start a background loop to re-run it every
     2 minutes for the rest of the session
  10. Launch metrics_api.py (FastAPI/uvicorn) in its own window
  11. Launch transaction_generator.py in its own window
  12. Launch streaming_engine.py in its own window

NOTE: graph_analytics.py (PageRank/betweenness/Louvain/FastRP/wash-ring
detection) is a periodic job, not a streaming one — this script runs it
once at startup and then starts a background loop that re-runs it every
2 minutes for the rest of the session (see README "Keeping Graph
Analytics Fresh").

Usage (from the project root, in PowerShell):
    powershell -ExecutionPolicy Bypass -File launch.ps1
#>

# Deliberately NOT setting $ErrorActionPreference = "Stop" globally — see
# the original script's note: native tools routinely write benign,
# expected output to stderr, so every native command below is followed by
# an explicit $LASTEXITCODE check instead.

$ProjectRoot = $PSScriptRoot

function Write-Step($msg) {
    Write-Host "`n==> $msg" -ForegroundColor Cyan
}

# ---------------------------------------------------------------------------
# 0. Load .env (docker-compose reads this file itself; PowerShell doesn't,
#    so this makes NEO4J_PASSWORD etc. available to the python scripts
#    this same script launches below).
# ---------------------------------------------------------------------------
Write-Step "Loading .env"
$envFile = Join-Path $ProjectRoot ".env"
if (-not (Test-Path $envFile)) {
    throw ".env not found. Copy .env.example to .env and set NEO4J_PASSWORD before running this script."
}
Get-Content $envFile | ForEach-Object {
    if ($_ -match '^\s*([^#=\s][^=]*)\s*=\s*(.*)\s*$') {
        [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2], "Process")
    }
}
if (-not $env:NEO4J_PASSWORD -or $env:NEO4J_PASSWORD -eq "change-me-before-running") {
    throw "NEO4J_PASSWORD is unset or still the placeholder value in .env. Set a real password before running this script."
}

# ---------------------------------------------------------------------------
# 1. Pre-flight checks
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

$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path

$requiredPackages = @("pyspark", "kafka", "numpy", "requests", "fastapi", "uvicorn", "neo4j", "sklearn", "joblib", "dotenv")
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

$javaHomeValid = $env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME "bin\java.exe"))
if (-not $javaHomeValid) {
    $javaProps = & java -XshowSettings:properties -version 2>&1
    $javaHomeLine = $javaProps | Select-String "^\s*java\.home\s*="
    if ($javaHomeLine) {
        $detectedJavaHome = ($javaHomeLine.ToString() -replace ".*java\.home\s*=\s*", "").Trim()
        if (Test-Path (Join-Path $detectedJavaHome "bin\java.exe")) {
            $env:JAVA_HOME = $detectedJavaHome
            Write-Host "  Detected JAVA_HOME: $env:JAVA_HOME" -ForegroundColor DarkGray
        } else {
            Write-Host "WARNING: java.home reported '$detectedJavaHome' but no bin\java.exe found there. PySpark will likely fail." -ForegroundColor Yellow
        }
    } else {
        Write-Host "WARNING: could not determine JAVA_HOME from 'java -XshowSettings:properties'. PySpark will likely fail." -ForegroundColor Yellow
    }
}
if ($env:JAVA_HOME -and ($env:PATH -notlike "*$env:JAVA_HOME\bin*")) {
    $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
}

# ---------------------------------------------------------------------------
# 2. Train the Isolation Forest model (idempotent — trains on synthetic
#    data with a fixed random seed, so re-running produces the same
#    model; skipped if isoforest_model.joblib already exists so restarts
#    of the pipeline don't retrain from scratch every time for no reason)
# ---------------------------------------------------------------------------
$modelPath = Join-Path $ProjectRoot "isoforest_model.joblib"
if (Test-Path $modelPath) {
    Write-Step "Isolation Forest model already exists, skipping training (delete isoforest_model.joblib to force a retrain)"
} else {
    Write-Step "Training Isolation Forest model (isoforest_model.joblib not found)"
    python "$ProjectRoot\train_isolation_forest.py"
    if ($LASTEXITCODE -ne 0) {
        throw "train_isolation_forest.py failed (exit $LASTEXITCODE)."
    }
}

# ---------------------------------------------------------------------------
# 3. Start Kafka + Zookeeper + Neo4j + Grafana
# ---------------------------------------------------------------------------
Write-Step "Starting Zookeeper + Kafka + Neo4j + Grafana (docker-compose up -d)"
docker-compose -f "$ProjectRoot\docker-compose.yml" up -d
if ($LASTEXITCODE -ne 0) {
    throw "docker-compose up -d failed (exit $LASTEXITCODE). Check Docker Desktop is running."
}

# ---------------------------------------------------------------------------
# 4. Wait for Kafka to accept connections
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
# 5. Wait for Neo4j's Bolt port to accept connections
# ---------------------------------------------------------------------------
Write-Step "Waiting for Neo4j to become ready"
$neo4jReady = $false
for ($i = 0; $i -lt 30; $i++) {
    docker exec neo4j cypher-shell -u $env:NEO4J_USER -p $env:NEO4J_PASSWORD "RETURN 1" *> $null
    if ($LASTEXITCODE -eq 0) {
        $neo4jReady = $true
        break
    }
    Write-Host "  Neo4j not ready yet, retrying in 2s... ($($i + 1)/30)"
    Start-Sleep -Seconds 2
}
if (-not $neo4jReady) {
    throw "Neo4j did not become ready in time. Check 'docker logs neo4j'."
}

# ---------------------------------------------------------------------------
# 6. Create the topic (idempotent)
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
# 7. Generate wallet profiles and load them into Neo4j
# ---------------------------------------------------------------------------
Write-Step "Generating synthetic wallet profiles"
python "$ProjectRoot\generate_wallet_profiles.py"
if ($LASTEXITCODE -ne 0) {
    throw "generate_wallet_profiles.py failed (exit $LASTEXITCODE)."
}

Write-Step "Loading wallet profiles into Neo4j (and provisioning schema)"
python "$ProjectRoot\load_wallet_profiles.py"
if ($LASTEXITCODE -ne 0) {
    throw "load_wallet_profiles.py failed (exit $LASTEXITCODE)."
}

# ---------------------------------------------------------------------------
# 8. Sanity-check the broadcast join
# ---------------------------------------------------------------------------
Write-Step "Verifying broadcast hash join"
python "$ProjectRoot\broadcast_engine.py"
if ($LASTEXITCODE -ne 0) {
    throw "broadcast_engine.py failed (exit $LASTEXITCODE)."
}

# ---------------------------------------------------------------------------
# 9. Run graph analytics once (PageRank / betweenness / Louvain / FastRP / wash-ring detection)
# ---------------------------------------------------------------------------
Write-Step "Running initial graph analytics pass (PageRank, betweenness, Louvain, FastRP, wash-ring detection)"
python "$ProjectRoot\graph_analytics.py"
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: graph_analytics.py failed. Confirm the GDS plugin loaded (docker logs neo4j)." -ForegroundColor Yellow
}

Write-Step "Launching periodic graph analytics refresh (every 2 minutes) in a new window"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; while (`$true) { python graph_analytics.py; Start-Sleep -Seconds 120 }"

# ---------------------------------------------------------------------------
# 10. Launch the metrics API (FastAPI/uvicorn) in its own window
# ---------------------------------------------------------------------------
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
# 11. Launch the producer and the streaming engine in their own windows
# ---------------------------------------------------------------------------
Write-Step "Launching transaction_generator.py in a new window"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; python transaction_generator.py"

Start-Sleep -Seconds 2

Write-Step "Launching streaming_engine.py in a new window"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; python streaming_engine.py"

Write-Host "`nPipeline started. Four new windows are now running: metrics_api, the producer, the streaming engine, and the periodic graph analytics refresh." -ForegroundColor Green
Write-Host "  Metrics API:      http://localhost:8000/metrics/summary" -ForegroundColor Green
Write-Host "  Graph summary:    http://localhost:8000/graph/summary" -ForegroundColor Green
Write-Host "  Wash rings:       http://localhost:8000/graph/wash-rings" -ForegroundColor Green
Write-Host "  Top risk wallets: http://localhost:8000/graph/top-risk-wallets" -ForegroundColor Green
Write-Host "  Live dashboard:   open graph_dashboard.html directly in a browser" -ForegroundColor Green
Write-Host "  Neo4j Browser:    http://localhost:7474  (see .env for credentials)" -ForegroundColor Green
Write-Host "  Grafana:          http://localhost:3000  (admin / admin)" -ForegroundColor Green
Write-Host "  Once some traffic has flowed, run 'python evaluate_model.py' for precision/recall against ground truth." -ForegroundColor Green