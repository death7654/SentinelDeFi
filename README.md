# SentinelDeFi

A real-time DeFi transaction processing and anomaly detection engine powered by **PySpark Structured Streaming**, **Apache Kafka**, **Delta Lake**, and a **KMeans** ML anomaly signal — with a live **Grafana** dashboard on top.

---

## Architecture at a Glance

```
transaction_generator.py  --(Kafka: defi-transactions)-->  streaming_engine.py
                                                                  |  |
                                              CEP rules + KMeans model (train_kmeans.py)
                                                                  |  |
                                broadcast join (broadcast_engine.py, wallet_profiles.csv)
                                                                  |  |
                                       Delta Lake (storage_layer.py)  metrics_api.py (FastAPI)
                                       sentineldefi.anomalies              |
                                                                       Grafana (Infinity datasource)
```

`streaming_engine.py` is the core: it reads transactions from Kafka, computes a
per-wallet sliding-window outlier statistic (CEP rules), scores each window
with a KMeans model as a second ML-based anomaly signal, joins in wallet risk
history, writes the enriched result to a Delta Lake table, **and** pushes each
micro-batch's stats to `metrics_api.py` over HTTP so Grafana can chart it live.

---

## Prerequisites & Setup

Before starting the pipeline, ensure the following components are installed and configured on your Windows machine:

### 1. Java Development Kit (JDK)
* **JDK 17** or **JDK 21** installed and configured in your environment variables.
* Verify with:
  ```powershell
  java -version
  ```

### 2. Hadoop Binaries (`winutils` & `hadoop.dll`)

PySpark on Windows requires native Hadoop binaries to handle filesystem checkpoints.

1. Download Hadoop binaries (Hadoop 3.x) and place them in `C:\hadoop\bin`.
2. Ensure `C:\hadoop\bin\winutils.exe` and `C:\hadoop\bin\hadoop.dll` exist.
3. Set your environment variables:
```powershell
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[System.Environment]::SetEnvironmentVariable("Path", $env:Path + ";C:\hadoop\bin", "User")
```

### 3. Docker Desktop

Kafka, Zookeeper, and Grafana all run as containers via `docker-compose.yml`.
Install Docker Desktop and make sure it's running before you start.

---

## Python Dependencies

Install everything the project actually imports in one shot:

```powershell
pip install -r requirements.txt
```

This installs `pyspark`, `kafka-python`, `numpy`, and `requests` for the core
pipeline, `fastapi`/`uvicorn` for the metrics API that powers the Grafana
dashboard, and `pandas`/`pyarrow`/`deltalake` for the standalone
`inspect_data.py` reader.

*(Note: the `spark-sql-kafka` and `delta-spark` **Spark/JVM** connector jars
are not pip packages — PySpark resolves them automatically via
`spark.jars.packages`, using the versions centralized in `storage_layer.py`.)*

---

## Quick Start (recommended)

Once dependencies are installed, `launch.ps1` automates everything except
training the KMeans model (a one-time step) and wiring up the Grafana panel
(also one-time). From the project root:

```powershell
python train_kmeans.py               # one-time: trains kmeans_model/ (see Step 4 below)
powershell -ExecutionPolicy Bypass -File launch.ps1
```

`launch.ps1` will, in order: start Kafka/Zookeeper/Grafana via Docker Compose,
wait for Kafka to be ready, create the `defi-transactions` topic, generate and
stage synthetic wallet profiles, provision the Hive database and Delta anomaly
table, sanity-check the broadcast join, then launch `metrics_api.py`,
`transaction_generator.py`, and `streaming_engine.py` each in their own
PowerShell window.

Once it finishes, jump straight to **[Live Grafana Dashboard](#live-grafana-dashboard)** below.

---

## Manual Step-by-Step (for understanding / troubleshooting)

If you'd rather run each stage yourself instead of `launch.ps1`, follow these
in order — every step here is something `launch.ps1` does automatically.

### Step 1: Start Kafka, Zookeeper & Grafana

```powershell
docker-compose up -d
```

This brings up Zookeeper, Kafka (broker on `localhost:9092`), Spark
master/worker (optional — the pipeline itself runs Spark locally via
`local[*]`, these are just available if you want to point it at a real
cluster later), and Grafana (`localhost:3000`, with the Infinity datasource
plugin pre-installed).

### Step 2: Create the Kafka Topic

```powershell
docker exec kafka kafka-topics --create --if-not-exists --topic defi-transactions --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1
```

To inspect partition offsets and active traffic:

```powershell
docker exec kafka kafka-topics --describe --topic defi-transactions --bootstrap-server localhost:9092
```

### Step 3: Generate Wallet Profiles & Provision Storage

```powershell
python generate_wallet_profiles.py
```

This writes `wallet_profiles.csv` next to the project scripts. Copy it to
`C:\sentineldefi\wallet_profiles.csv` (where `storage_layer.py`'s fallback
path expects it), then provision the Hive database and Delta anomaly table:

```powershell
New-Item -ItemType Directory -Force -Path C:\sentineldefi | Out-Null
Copy-Item -Force .\wallet_profiles.csv C:\sentineldefi\wallet_profiles.csv
python storage_layer.py
```

Optionally sanity-check the broadcast hash join before starting the stream:

```powershell
python broadcast_engine.py
```

> **Important:** `streaming_engine.py` imports `broadcast_engine.py`, which
> needs either the Hive table or `wallet_profiles.csv` to already exist. Skip
> this step and the streaming engine will crash on startup with a
> file-not-found error.

### Step 4: Train the KMeans Anomaly Model (one-time, before first run)

`streaming_engine.py` loads a KMeans model as a second, learned anomaly
signal alongside the rule-based CEP thresholds. Train it once (re-run any
time you want to refit against updated synthetic parameters):

```powershell
python train_kmeans.py
```

This saves the fitted `PipelineModel` to `kmeans_model/` and its cluster
metadata to `kmeans_model_meta.json`, both alongside the project scripts.
If this step is skipped, `streaming_engine.py` still runs — it logs a
warning and falls back to rule-based detection only, and the dashboard's
`ml_cluster`/`ml_anomaly` fields stay null.

### Step 5: Start the Metrics API

```powershell
python -m uvicorn metrics_api:app --host 0.0.0.0 --port 8000
```

This is a small FastAPI app that `streaming_engine.py` posts each
micro-batch's stats to, and that Grafana polls for the live dashboard.
**Start it before Step 7** — `streaming_engine.py` will still run without it
(it just logs a failed-push warning per batch), but you'll see nothing in
Grafana.

Verify it's up:

```powershell
curl http://localhost:8000/health
```

### Step 6: Run the Transaction Producer

Start the Kafka producer that simulates incoming DeFi transaction streams
(normal traffic, plus flash-loan, wash-trading, and bot-burst anomalies):

```powershell
python transaction_generator.py
```

*Expected output:*

```text
[normal] 0x00000000... | $224.18
[flash-loan] 0x00000000... | $4,812,940.11
```

### Step 7: Run the PySpark Streaming Engine

In a new terminal window, start the streaming analysis pipeline:

```powershell
python streaming_engine.py
```

*Expected output:*

```text
PySpark 3.5.3 Streaming Engine Started...

-------------------------------------------
Batch: 0
-------------------------------------------
+----------------------------------+----------------------------------+----------+-------+--------------------------+
|tx_id                             |wallet_address                    |amount_usd|gas_fee|timestamp                 |
+----------------------------------+----------------------------------+----------+-------+--------------------------+
|0xcb70fb75a4a9a2c3f829d733ea556de...|0x00000000000000000000000000000005|715.52    |0.0118 |2026-08-02 19:08:03.810916|
+----------------------------------+----------------------------------+----------+-------+--------------------------+
```

---

## Live Grafana Dashboard

Grafana (`docker-compose.yml`) comes up with the
[Infinity datasource plugin](https://github.com/yesoreyeram/yesoreyeram-infinity-datasource)
pre-installed via `GF_INSTALL_PLUGINS`, which lets Grafana poll a plain JSON
HTTP endpoint on a timer — exactly what `metrics_api.py`'s
`GET /metrics/summary` returns. This is how you see the ML output live,
without needing a time-series database.

**One thing to get right:** Grafana itself runs *inside* a Docker container,
so from Grafana's point of view the metrics API (running natively on your
Windows host) is at `http://host.docker.internal:8000`, **not**
`localhost:8000`. Use `localhost:8000` only when hitting the API yourself
from a browser or `curl` on the host.

### 1. Open Grafana and add the Infinity datasource

1. Go to `http://localhost:3000` and log in (`admin` / `admin`).
2. **Connections → Data sources → Add data source → Infinity**.
3. Leave auth as "None" and click **Save & test**.

### 2. Build a live "ML anomaly feed" table panel

1. **Dashboards → New → New Dashboard → Add visualization → Infinity**.
2. Panel settings:
   - **Type:** JSON
   - **Source:** URL
   - **URL:** `http://host.docker.internal:8000/metrics/summary`
   - **Root / Rows selector:** `recent_records`
   - **Columns:** add `wallet_address`, `amount_usd`, `z_score`,
     `anomaly_reason`, `ml_cluster`, `ml_anomaly` (these are exactly the
     columns `streaming_engine.py` computes per micro-batch — see
     [Streaming Data Schema](#streaming-data-schema) below).
3. Set the panel visualization to **Table**.
4. Set the dashboard's refresh interval (top right) to `5s` so it keeps
   polling `metrics_api.py` as new batches arrive.

### 3. Add stat panels for the headline numbers

Repeat "Add visualization → Infinity" for a few single-value **Stat**
panels, same URL, **Type: JSON**, **Root selector** left empty (root object),
picking one field each:

- `total_processed` — total transactions processed
- `total_anomalies` — total anomalies flagged (rule-based + ML)
- `avg_z_score` — running average outlier statistic
- `status` — `active` once the streaming engine is pushing batches

Save the dashboard. With `transaction_generator.py` and `streaming_engine.py`
both running, you should see the table and stat panels update every few
seconds as new anomalies (including `ML_CLUSTER_ANOMALY` rows once
`train_kmeans.py` has been run) flow through.

---

## Streaming Data Schema

Incoming JSON byte payloads are deserialized using the following Spark schema:

| Column Field | Data Type | Description |
| --- | --- | --- |
| `tx_id` | `StringType` | Hexadecimal transaction hash identifier |
| `wallet_address` | `StringType` | Sender wallet address |
| `amount_usd` | `DoubleType` | Transaction value in USD |
| `gas_fee` | `DoubleType` | Network execution gas fee |
| `timestamp` | `TimestampType` | Event execution time |

The enriched output written to `sentineldefi.anomalies` (and pushed to
`metrics_api.py`) adds the CEP and ML columns computed downstream in
`streaming_engine.py`:

| Column Field | Data Type | Description |
| --- | --- | --- |
| `z_score` | `DoubleType` | Grubbs'-test-style outlier statistic: `(max_amount - mean_amount) / stddev_amount` for the window |
| `anomaly_reason` | `StringType` | `LARGE_SINGLE_TRANSACTION`, `DYNAMIC_Z_SCORE_SPIKE`, `BOT_BURST_HIGH_FREQUENCY`, `ML_CLUSTER_ANOMALY`, or null |
| `ml_cluster` | `IntegerType` | KMeans cluster ID assigned by `kmeans_model/` (null if the model hasn't been trained yet) |
| `ml_anomaly` | `BooleanType` | Whether `ml_cluster` is one of the clusters `train_kmeans.py` identified as anomalous |

---

## Common Troubleshooting

### 1. `java.lang.UnsatisfiedLinkError: NativeIO$Windows.access0`

* **Cause:** Spark cannot link to `hadoop.dll`.
* **Fix:** Ensure `os.environ["HADOOP_HOME"] = r"C:\hadoop"` and `os.environ["PATH"] += r";C:\hadoop\bin"` are declared at the very top of `streaming_engine.py` before `SparkSession` is created.

### 2. `java.lang.NoSuchMethodError: SerializedOffset.<init>`

* **Cause:** Mismatch between installed `pyspark` package version and the requested Maven package (`spark-sql-kafka`).
* **Fix:** the Kafka connector version is now resolved in one place — `KAFKA_PACKAGE` in `storage_layer.py`, derived from `pyspark.__version__` (currently resolves to `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`, matching the `pyspark==3.5.3` pin in `requirements.txt`). Both `streaming_engine.py` and `storage_layer.get_spark_session()` import this constant instead of each hardcoding their own version, so the two Spark sessions can no longer drift apart. If you upgrade `pyspark`, this updates automatically for both.

### 3. `ModuleNotFoundError: No module named 'fastapi'` / `'requests'`

* **Cause:** these were missing from earlier versions of `requirements.txt` even though `metrics_api.py` and `streaming_engine.py` import them directly.
* **Fix:** `pip install -r requirements.txt` (now includes `requests`, `fastapi`, and `uvicorn`).

### 4. Grafana panel shows "No data" or a connection error

* **Cause:** almost always `http://localhost:8000` used from *inside* the Grafana container, which can't reach your Windows host under that hostname.
* **Fix:** use `http://host.docker.internal:8000/metrics/summary` in the Infinity datasource URL, not `localhost`. Also confirm `metrics_api.py` is actually running (`curl http://localhost:8000/health` from the host) and that `streaming_engine.py` has processed at least one batch.

### 5. `launch.ps1` dies immediately on `java -version` with a `NativeCommandError`

* **Cause:** `java -version` prints its version string to **stderr**, not stdout, even on success. Older versions of `launch.ps1` set `$ErrorActionPreference = "Stop"` globally, which makes PowerShell treat *any* stderr text from a native command as a terminating error — killing the script on the very first prerequisite check, regardless of exit code.
* **Fix:** `launch.ps1` no longer sets `$ErrorActionPreference = "Stop"` globally; every native command it runs is instead followed by an explicit `$LASTEXITCODE` check. If you still hit this, make sure you're running the current version of the script.

### 7. `SparkException: Python worker failed to connect back` / `Python was not found; run without arguments to install from the Microsoft Store...`

* **Cause:** Windows' "App Execution Alias" stub for `python` intercepted a Spark-spawned Python worker subprocess. This happens whenever a Spark session doesn't pin `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to the real interpreter — `spark.createDataFrame()` on a plain Python list (as `storage_layer.py` does to provision the empty Delta anomaly table) spawns a worker even with no UDFs involved.
* **Fix:** already applied in `storage_layer.py`'s `get_spark_session()` (mirroring what `streaming_engine.py`/`train_kmeans.py` already did). If you still hit this elsewhere, you can also disable the alias yourself: **Settings → Apps → Advanced app settings → App execution aliases → turn off "python.exe"/"python3.exe"**.

### 9. `storage_layer.py` fails immediately with `The system cannot find the path specified.`

* **Cause:** `storage_layer.py` points Derby's (the embedded Hive metastore's) log file at `C:\hadoop\logs\derby.log` but didn't create that directory first — Derby doesn't create it for you, and fails to open the log file with this bare, generic-looking message.
* **Fix:** already applied — `storage_layer.py` now creates `C:\hadoop\logs` before starting the Spark session, matching what `streaming_engine.py`/`train_kmeans.py` already did.

### 11. `launch.ps1` reports `Detected JAVA_HOME: C:\Program Files\Common Files\Oracle\Java` and the next step fails with a bare `The system cannot find the path specified.`

* **Cause:** Oracle's Java installer adds a `javapath` shim directory to `PATH` (commonly `C:\Program Files\Common Files\Oracle\Java\javapath`) containing just a `java.exe` stub — not a real JDK, no `bin`/`lib` structure. If that shim resolves ahead of your real JDK on `PATH`, naively walking up from `java.exe`'s location lands on a folder that isn't a JDK at all, so the next Spark session's JVM launch fails immediately with no Spark output.
* **Fix:** `launch.ps1` now determines `JAVA_HOME` by asking the running JVM for its own `java.home` property (`java -XshowSettings:properties -version`) instead of guessing from `java.exe`'s path — this resolves correctly through the shim to your real JDK. If you still see a wrong path detected, set `JAVA_HOME` yourself before running the script:
  ```powershell
  [System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\path\to\your\real\jdk-17", "User")
  ```

### 12. `streaming_engine.py` crashes on startup with a file-not-found / table-not-found error

* **Cause:** `broadcast_engine.py` needs `wallet_profiles.csv` (or the Hive table) to already exist.
* **Fix:** run `generate_wallet_profiles.py` and `storage_layer.py` first (Manual Step 3 above), or just use `launch.ps1`, which does this for you in the right order.
