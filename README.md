# SentinelDeFi

A real-time DeFi transaction processing and anomaly detection engine powered by **PySpark Structured Streaming**, **Apache Kafka**, a **Neo4j** graph database with **Graph Data Science (GDS)**, and an **Isolation Forest** ML anomaly signal — with a live **Grafana** dashboard on top.

---

## What changed from v1

The original version stored enriched transactions as flat rows in a **Delta Lake** table and scored anomalies with a **KMeans** model. Two things drove the rearchitecture:

1. **Flat tables can't represent relationships.** Wash trading is fundamentally a *graph* pattern — the same funds passed in a circle across several wallets. A flat table of per-window statistics has no way to ask "do these four wallets form a cycle?" without expensive self-joins. **Neo4j** stores wallets and transactions as an actual graph, and its **Graph Data Science (GDS)** library ships production-grade PageRank, betweenness centrality, Louvain community detection, FastRP node embeddings, and (via Cypher) cycle detection — the exact primitives fraud detection needs.
2. **KMeans's binary cluster assignment was a poor model of "anomalous."** It required guessing which of k clusters was "the anomalous one" by assuming the majority cluster is normal — an assumption that breaks under a coordinated attack, and gives no sense of *how* anomalous a point is. **Isolation Forest** gives every transaction a continuous anomaly score, makes no assumption that anomalies form a convex cluster, and folds in two graph-derived features that KMeans had no way to use.

A second pass then addressed gaps in the first rearchitecture itself:

- **No way to measure whether any of this actually works** — fixed by threading the generator's own ground truth through the whole pipeline into Neo4j and adding `evaluate_model.py` (see [Evaluating Detection Accuracy](#evaluating-detection-accuracy)).
- **Non-idempotent Neo4j writes** — a retried Spark micro-batch used to double-write the same edge, corrupting every GDS algorithm downstream. Writes are now `MERGE`d on a stable key instead of `CREATE`d (see [Idempotent Writes](#idempotent-writes)).
- **Wallet profiles were a frozen synthetic snapshot** — `Wallet.historical_tx_count` never changed no matter how much real traffic flowed through. Real activity is now tracked separately and updated live (see [Live Wallet Profiles](#live-wallet-profiles)).
- **PageRank + a cycle flag was a thin feature set** — added betweenness centrality (catches layering, not just cycles) and FastRP node embeddings (a learned structural-novelty signal, not another hand-picked heuristic).
- **No demo-able visual** — added `graph_dashboard.html`, a live force-directed view of the transaction graph and detected rings.
- **A hardcoded default password and an in-memory-only metrics store** — moved secrets to `.env` and persisted metrics to disk.

See [Architecture at a Glance](#architecture-at-a-glance) and [Graph-Native Fraud Detection](#graph-native-fraud-detection-the-next-level-piece) for the details.

---

## Architecture at a Glance

```
transaction_generator.py  --(Kafka: defi-transactions)-->  streaming_engine.py
   (wallet_address, to_wallet, true_label)                       |  |
                                              CEP rules + Isolation Forest (train_isolation_forest.py)
                                                                   |  |
                             broadcast join (broadcast_engine.py <- Neo4j: historical + live profile,
                                                                     graph_risk_score, structural_novelty_score)
                                                                   |  |
                                    Neo4j graph (graph_storage.py)   metrics_api.py (FastAPI, persisted state)
                                    (:Wallet)-[:SENT]->(:Wallet)          |         |
                                    idempotent MERGE on window_start   Grafana   graph_dashboard.html
                                       |                               (Infinity)  (live force-directed view)
                          graph_analytics.py (GDS: PageRank,
                          betweenness, Louvain, FastRP embeddings,
                          wash-ring cycle detection)
                          — periodic job, writes back onto Wallet nodes
                                       |
                          evaluate_model.py — precision/recall/F1
                          against true_label, read back from Neo4j
```

`streaming_engine.py` is still the core: it reads transactions from Kafka, computes a per-(wallet, counterparty) sliding-window outlier statistic (CEP rules), joins in each sender's historical profile *and* live graph risk score from Neo4j, scores the result with an Isolation Forest, writes each transaction as a graph edge to Neo4j, **and** pushes each micro-batch's stats to `metrics_api.py` over HTTP so Grafana can chart it live.

`graph_analytics.py` runs separately (periodically, not per-batch) and is what makes this a *graph-native* fraud detection pipeline rather than a relational one with extra steps: it projects the transaction graph in Neo4j GDS, runs PageRank and Louvain community detection, and runs a Cypher cycle-detection query to flag wallets sitting on a wash-trading ring — writing all three back onto `Wallet` nodes so the next streaming batch's broadcast join can use them as ML features.

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

PySpark on Windows requires native Hadoop binaries to handle filesystem checkpoints (Spark still writes its own streaming checkpoint state locally, even though the sink is Neo4j now).

1. Download Hadoop binaries (Hadoop 3.x) and place them in `C:\hadoop\bin`.
2. Ensure `C:\hadoop\bin\winutils.exe` and `C:\hadoop\bin\hadoop.dll` exist.
3. Set your environment variables:
```powershell
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[System.Environment]::SetEnvironmentVariable("Path", $env:Path + ";C:\hadoop\bin", "User")
```

### 3. Docker Desktop

Kafka, Zookeeper, Neo4j, and Grafana all run as containers via `docker-compose.yml`. Install Docker Desktop and make sure it's running before you start.

### 4. Configure secrets (`.env`)

```powershell
Copy-Item .env.example .env
```

Then open `.env` and set a real `NEO4J_PASSWORD` (anything you like — just not the placeholder). Both `docker-compose.yml` (via its own automatic `.env` support) and every Python script (via `graph_storage.py`'s `python-dotenv` load) read this same file, so they can't drift out of sync. `graph_storage.py` raises immediately with a clear message if `NEO4J_PASSWORD` isn't set anywhere — there is no hardcoded fallback password to accidentally rely on.

---

## Python Dependencies

```powershell
pip install -r requirements.txt
```

This installs `pyspark`, `kafka-python`, `numpy`, and `requests` for the core pipeline; `neo4j` (the official Bolt driver) for every graph read/write; `scikit-learn`/`joblib` for the Isolation Forest; `fastapi`/`uvicorn` for the metrics API; and `pandas` for the standalone `inspect_data.py` reader.

*(Note: the `spark-sql-kafka` **Spark/JVM** connector jar is not a pip package — PySpark resolves it automatically via `spark.jars.packages`, using the version centralized in `graph_storage.py`.)*

---

## Quick Start (recommended)

Make sure you've copied `.env.example` to `.env` and set a real password first (see above) — `launch.ps1` refuses to start otherwise.

```powershell
python train_isolation_forest.py      # one-time: trains isoforest_model.joblib (see Step 4 below)
powershell -ExecutionPolicy Bypass -File launch.ps1
```

`launch.ps1` will, in order: load `.env`, start Kafka/Zookeeper/Neo4j/Grafana via Docker Compose, wait for Kafka and Neo4j to be ready, create the `defi-transactions` topic, generate and load synthetic wallet profiles into Neo4j, sanity-check the broadcast join, run an initial graph analytics pass (and start a background loop re-running it every 2 minutes), then launch `metrics_api.py`, `transaction_generator.py`, and `streaming_engine.py` each in their own PowerShell window.

Once traffic has been flowing for a bit, open `graph_dashboard.html` directly in a browser for a live view, and run `python evaluate_model.py` to see actual precision/recall numbers.

---

## Manual Step-by-Step (for understanding / troubleshooting)

### Step 1: Start Kafka, Zookeeper, Neo4j & Grafana

```powershell
docker-compose up -d
```

Brings up Zookeeper, Kafka (broker on `localhost:9092`), **Neo4j** (Bolt on `localhost:7687`, Browser UI on `http://localhost:7474`, credentials `neo4j` / `sentineldefi123`, with the GDS plugin pre-installed), Spark master/worker (optional, unused by default — the pipeline runs Spark locally via `local[*]`), and Grafana (`localhost:3000`, Infinity datasource pre-installed).

### Step 2: Create the Kafka Topic

```powershell
docker exec kafka kafka-topics --create --if-not-exists --topic defi-transactions --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1
```

### Step 3: Generate Wallet Profiles & Load Them Into Neo4j

```powershell
python generate_wallet_profiles.py
python load_wallet_profiles.py
```

`load_wallet_profiles.py` also provisions the Neo4j schema (a uniqueness constraint on `Wallet.address` plus supporting indexes) — this replaces `storage_layer.py`'s old `provision_hive_database()` / `register_wallet_profiles_table()`.

Optionally sanity-check the broadcast join before starting the stream:

```powershell
python broadcast_engine.py
```

> **Important:** `streaming_engine.py` imports `broadcast_engine.py`, which needs Neo4j reachable and at least schema-initialized. Skip Step 3 and the streaming engine will still run (the join just returns an empty context), but every row's historical/graph fields will be null.

### Step 4: Train the Isolation Forest Model (one-time, before first run)

```powershell
python train_isolation_forest.py
```

Saves the fitted model to `isoforest_model.joblib` and its metadata (feature list, contamination rate, decision threshold) to `isoforest_model_meta.json`. If this step is skipped, `streaming_engine.py` still runs — it logs a warning and falls back to rule-based detection only, and `ml_score`/`ml_anomaly` stay null/false.

### Step 5: Run an Initial Graph Analytics Pass

```powershell
python graph_analytics.py
```

Projects the transaction graph in Neo4j GDS and runs PageRank, Louvain community detection, and wash-trading ring (cycle) detection, writing the results onto `Wallet` nodes. Run this once before starting the stream so the very first broadcast join has real graph_risk_score values instead of all-zero defaults, and **re-run it periodically** while the pipeline is running — see [Keeping Graph Analytics Fresh](#keeping-graph-analytics-fresh).

### Step 6: Start the Metrics API

```powershell
python -m uvicorn metrics_api:app --host 0.0.0.0 --port 8000
```

Same as before, plus three new endpoints (see [New Metrics API Endpoints](#new-metrics-api-endpoints)).

### Step 7: Run the Transaction Producer

```powershell
python transaction_generator.py
```

Each payload now also carries a `to_wallet` — normal traffic settles against one of three simulated DEX/lending contracts, flash-loan anomalies hit a lending pool, bot-burst anomalies hammer a contract, and wash-trading anomalies form an actual directed cycle across a 4-wallet ring (`A -> B -> C -> D -> A`), which is what makes them detectable as a *graph* cycle and not just a statistical spike.

### Step 8: Run the PySpark Streaming Engine

```powershell
python streaming_engine.py
```

### Step 9: Watch It Live, Then Check the Numbers

Open `graph_dashboard.html` directly in a browser for a live force-directed view. After a few minutes of traffic, run:

```powershell
python evaluate_model.py
```

to see actual precision/recall/F1 against the generator's own ground truth (see [Evaluating Detection Accuracy](#evaluating-detection-accuracy)).

---

## Graph-Native Fraud Detection (the "next level" piece)

This is the part a flat table genuinely could not do. `graph_analytics.py` runs three algorithms via Neo4j GDS:

| Algorithm | Writes | What it catches |
| --- | --- | --- |
| **PageRank** | `Wallet.pagerank` | Money-flow importance — a wallet receiving heavily from other important wallets ranks higher, regardless of any single transaction's size |
| **Louvain community detection** | `Wallet.community_id` | Wallets that transact heavily with each other cluster together — an isolated clique trading mostly with itself is a stronger anomaly signal than transaction size alone |
| **Cycle detection** (plain Cypher, 3–8 hop directed trails) | `Wallet.in_wash_ring`, `Wallet.wash_ring_id` | The structural signature of wash trading — funds actually looping back to their origin, which a per-wallet CEP window can never see because it only ever looks at one wallet in isolation |

`broadcast_engine.py` turns these into two scalar ML features:

- **`graph_risk_score`** — min-max-normalized PageRank and betweenness blended with a flat bump for wash-ring membership. Betweenness matters here specifically because it catches **layering** (funds routed `A -> B -> C -> D` where `D` cashes out, with no loop at all) that the cycle-detection query above is structurally blind to — a pure pass-through wallet doesn't accumulate PageRank "importance" either, since it moves money on immediately rather than holding it.
- **`structural_novelty_score`** — each wallet's FastRP embedding distance from the graph-wide centroid embedding, min-max normalized. This one is deliberately *not* hand-engineered from a specific graph property; FastRP learns a fixed-length structural fingerprint per wallet from its neighborhood, so this catches "this wallet's connections have an unusual shape" without us having had to decide in advance what "unusual" means.

`train_isolation_forest.py` uses both alongside the existing `z_score`, `tx_count`, and `avg_gas_fee` — five features total.

### Keeping Graph Analytics Fresh

`graph_analytics.py` is a periodic batch job, not a streaming one — GDS graph projection isn't designed to run at micro-batch latency. For a live demo, run it on a timer alongside the pipeline, e.g. in its own PowerShell window:

```powershell
while ($true) { python graph_analytics.py; Start-Sleep -Seconds 120 }
```

`broadcast_engine.py` caches the Neo4j read for 30 seconds, so a 2-minute refresh cadence is more than fast enough for it to pick up new graph_risk_score values without hammering Neo4j on every micro-batch. `launch.ps1` starts this refresh loop for you automatically in its own window.

---

## Idempotent Writes

Spark's `foreachBatch` makes no exactly-once guarantee to an arbitrary sink — a micro-batch can be retried after a transient failure. The first version of `graph_storage.py` used `CREATE` for every `SENT` relationship, so a retry meant the same edge got written twice: `tx_count` and `amount_usd` would double, and every GDS algorithm downstream (PageRank weights, community sizes, cycle counts) would be silently wrong.

`write_transactions_batch()` now `MERGE`s the relationship on `window_start` — which is exactly `streaming_engine.py`'s own `groupBy` key (`window`, `wallet_address`, `to_wallet`) — instead of `CREATE`ing it. A retried batch matches the existing relationship and overwrites it with the same computed values, instead of duplicating it.

---

## Live Wallet Profiles

`Wallet.historical_tx_count` and `Wallet.historical_risk_tier` are still what they were originally: a one-time synthetic backstory loaded by `load_wallet_profiles.py`, meant to simulate "this wallet's history before we started watching." They intentionally never change after that.

What's new: `Wallet.live_tx_count` and `Wallet.last_seen_ts`, updated on every batch by `write_transactions_batch()`, tracking what a wallet has *actually done* since the pipeline started running. A wallet whose `live_tx_count` has exploded relative to a modest `historical_tx_count` is a meaningfully different signal than either number alone — that comparison wasn't possible when the only tx-count field in the graph was a frozen seed value.

---

## Evaluating Detection Accuracy

Every claim this pipeline makes about detecting fraud is checkable, because `transaction_generator.py` already knows the ground truth at the moment it emits each transaction — it's the thing deciding to emit a `flash_loan` vs. a `wash_trade` vs. ordinary traffic. That label (`true_label`) now rides along through Kafka, through the Spark aggregation, and onto each `SENT` relationship in Neo4j.

```powershell
python evaluate_model.py
```

This reads labeled transactions back out of Neo4j and reports precision, recall, F1, and a confusion matrix for three detectors separately:

- **Rule-based CEP only** (the `LARGE_SINGLE_TRANSACTION` / `DYNAMIC_Z_SCORE_SPIKE` / `BOT_BURST_HIGH_FREQUENCY` thresholds)
- **Isolation Forest only** (`ml_anomaly`)
- **Combined** (`anomaly_reason` set at all — what actually ships today)

against the label `normal` vs. everything else. It also writes the full report to `eval_report.json`. Run it after letting `transaction_generator.py` + `streaming_engine.py` run for a few minutes — at this project's ~2% anomaly rate you want at least a few hundred transactions before the precision/recall numbers mean anything. If a number looks bad, that's the CEP threshold constants in `graph_storage.py` or the Isolation Forest's `CONTAMINATION` in `train_isolation_forest.py` telling you where to look next, not a sign something is broken.

---

## Live Graph Dashboard

`graph_dashboard.html` is a standalone page (vis-network via CDN, no build step) that polls `metrics_api.py` every 5 seconds and renders the top wallets by PageRank as a force-directed graph, coloring wash-ring members red and high-PageRank hubs yellow, with a live rings list and top-risk-wallets panel alongside it. Just open the file directly in a browser once `metrics_api.py` is running — no server needed. This is meant to be the thing you actually have open during a demo or presentation; Grafana's tables are useful for the numeric time series, but they don't make a detected ring visually obvious the way this does.

(`metrics_api.py` has CORS wide open to make this work from a `file://` page. That's fine for a local class project talking only to `localhost` — don't carry that setting into anything deployed.)

---

## New Metrics API Endpoints

Alongside the original `/metrics/summary`:

| Endpoint | Returns |
| --- | --- |
| `GET /graph/wash-rings` | Every wallet currently flagged on a detected wash-trading cycle, grouped by ring id |
| `GET /graph/top-risk-wallets?limit=20` | Top wallets by PageRank, with community and wash-ring status — the "look here first" view for a human analyst |
| `GET /graph/summary` | Headline graph stats: wallet count, edge count, ring-flagged wallet count, community count |

These are meant to back Grafana Infinity panels exactly the way `/metrics/summary` already does — add a Table panel pointed at `http://host.docker.internal:8000/graph/top-risk-wallets` the same way the original dashboard used `/metrics/summary`.

---

## Streaming Data Schema

Incoming JSON byte payloads (now including a counterparty):

| Column Field | Data Type | Description |
| --- | --- | --- |
| `tx_id` | `StringType` | Hexadecimal transaction hash identifier |
| `wallet_address` | `StringType` | Sender wallet address |
| `to_wallet` | `StringType` | Receiving wallet or contract address — this is what makes the transaction a graph edge |
| `amount_usd` | `DoubleType` | Transaction value in USD |
| `gas_fee` | `DoubleType` | Network execution gas fee |
| `true_label` | `StringType` | Generator's own ground truth: `normal`, `flash_loan`, `wash_trade`, or `bot_burst` — used only by `evaluate_model.py` |
| `timestamp` | `TimestampType` | Event execution time |

The graph edges written to Neo4j (`(:Wallet)-[:SENT]->(:Wallet)`) carry the CEP and ML enrichment, aggregated per **(sender, receiver, window)** — so each aggregate maps onto exactly one graph edge — and `MERGE`d on `window_start` for idempotency (see [Idempotent Writes](#idempotent-writes)):

| Property | Type | Description |
| --- | --- | --- |
| `z_score` | float | Grubbs'-test-style outlier statistic for this wallet-pair's window |
| `anomaly_reason` | string | `LARGE_SINGLE_TRANSACTION`, `DYNAMIC_Z_SCORE_SPIKE`, `BOT_BURST_HIGH_FREQUENCY`, `ML_ISOLATION_FOREST_ANOMALY`, or null |
| `ml_score` | float | Isolation Forest decision score (more negative = more anomalous); null if the model hasn't been trained yet |
| `ml_anomaly` | bool | Whether `ml_score` is below the trained threshold |
| `true_label` | string | Ground truth carried through from the generator, for `evaluate_model.py` |

---

## Common Troubleshooting

### 1. `java.lang.UnsatisfiedLinkError: NativeIO$Windows.access0`

* **Cause:** Spark cannot link to `hadoop.dll`.
* **Fix:** Ensure `os.environ["HADOOP_HOME"] = r"C:\hadoop"` and `os.environ["PATH"] += r";C:\hadoop\bin"` are declared at the very top of `streaming_engine.py` before `SparkSession` is created.

### 2. `java.lang.NoSuchMethodError: SerializedOffset.<init>`

* **Cause:** Mismatch between installed `pyspark` package version and the requested Maven package (`spark-sql-kafka`).
* **Fix:** the Kafka connector version is resolved in one place — `KAFKA_PACKAGE` in `graph_storage.py`, derived from `pyspark.__version__`. `streaming_engine.py` imports this constant instead of hardcoding its own version.

### 3. `ModuleNotFoundError: No module named 'neo4j'` / `'sklearn'` / `'dotenv'`

* **Cause:** these were added to `requirements.txt` as part of the Neo4j/Isolation Forest/`.env` rearchitecture.
* **Fix:** `pip install -r requirements.txt`.

### 4. `RuntimeError: NEO4J_PASSWORD is not set`

* **Cause:** no `.env` file exists yet, or it does but wasn't picked up (wrong working directory, or the variable name is misspelled).
* **Fix:** `Copy-Item .env.example .env`, then edit `.env` and set a real `NEO4J_PASSWORD`. Every script that touches Neo4j imports `graph_storage.py`, which loads `.env` via `python-dotenv` — if this error is still firing, confirm `.env` is in the same directory you're running the script from.

### 5. `streaming_engine.py` logs "Could not load Isolation Forest model"

* **Cause:** `train_isolation_forest.py` hasn't been run yet (or `isoforest_model.joblib` was deleted).
* **Fix:** run `python train_isolation_forest.py` once. The pipeline still runs without it — it just falls back to rule-based CEP detection only.

### 6. `graph_analytics.py` fails with a procedure-not-found error for `gds.*`

* **Cause:** the GDS plugin didn't load, usually because `docker-compose.yml`'s `NEO4J_PLUGINS`/allowlist environment variables weren't picked up (e.g. a stale Neo4j container from before this rearchitecture).
* **Fix:** `docker-compose down` then `docker-compose up -d` to recreate the `neo4j` container fresh, and check `docker logs neo4j` for a line confirming the `graph-data-science` plugin installed.

### 7. Every row's `historical_risk_tier` / `graph_risk_score` / `structural_novelty_score` comes back as `unknown` / `0.0`

* **Cause:** `load_wallet_profiles.py` and/or `graph_analytics.py` haven't been run yet, so the wallets referenced by live traffic don't exist in Neo4j with any properties set. `structural_novelty_score` specifically needs `graph_analytics.py`'s FastRP step to have run at least once — a wallet with no `embedding` property yet defaults to 0.0 novelty rather than being dropped.
* **Fix:** run both once before starting `streaming_engine.py` (see Steps 3 and 5 above); `launch.ps1` does this for you in order, and also starts the periodic 2-minute refresh loop so this doesn't go stale.

### 8. `metrics_api.py`'s totals reset to 0 after a restart

* **Cause:** you're running an older copy — this was the in-memory-only `metrics_store` from the first Neo4j rearchitecture pass.
* **Fix:** current `metrics_api.py` persists to `metrics_state.json` next to it and reloads on startup. If totals are still resetting, check that the process has write permission to that directory, and look for a `[WARN] Failed to persist metrics state` line in its console output.

### 9. Grafana panel shows "No data" or a connection error

* **Cause:** almost always `http://localhost:8000` used from *inside* the Grafana container, which can't reach your Windows host under that hostname.
* **Fix:** use `http://host.docker.internal:8000/...` in the Infinity datasource URL, not `localhost`.

### 10. `graph_dashboard.html` shows "disconnected" and never loads data

* **Cause:** either `metrics_api.py` isn't running, or it's running but CORS got stripped somehow (e.g. an older `metrics_api.py` from before `CORSMiddleware` was added).
* **Fix:** confirm `curl http://localhost:8000/health` works from the host first. If that's fine but the dashboard still shows disconnected, check the browser's console for a CORS error specifically and confirm `metrics_api.py` includes the `app.add_middleware(CORSMiddleware, ...)` block.

### 11. `SparkException: Python worker failed to connect back` / `Python was not found; run without arguments to install from the Microsoft Store...`

* **Cause:** Windows' "App Execution Alias" stub for `python` intercepted a Spark-spawned Python worker subprocess.
* **Fix:** already applied in `graph_storage.py`'s `get_spark_session()`. If you still hit this elsewhere, disable the alias: **Settings → Apps → Advanced app settings → App execution aliases → turn off "python.exe"/"python3.exe"**.

### 12. `launch.ps1` dies immediately on `java -version` with a `NativeCommandError`

* **Cause:** `java -version` prints its version string to **stderr**, not stdout, even on success, and `$ErrorActionPreference = "Stop"` promotes that into a terminating error.
* **Fix:** `launch.ps1` doesn't set `$ErrorActionPreference = "Stop"` globally; every native command is followed by an explicit `$LASTEXITCODE` check.

### 13. `launch.ps1` reports a bad `JAVA_HOME` and the next step fails with a bare `The system cannot find the path specified.`

* **Cause:** Oracle's Java installer's `javapath` shim resolving ahead of the real JDK on `PATH`.
* **Fix:** `launch.ps1` asks the running JVM for its own `java.home` property instead of guessing from `java.exe`'s path. If it still detects wrong, set `JAVA_HOME` yourself:
  ```powershell
  [System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\path\to\your\real\jdk-17", "User")
  ```

---

## Other Ideas To Go Even Further

Most of the gaps identified in the first Neo4j rearchitecture pass — no accuracy measurement, non-idempotent writes, a frozen wallet profile, a thin graph feature set, no demo-able visual, a hardcoded password, an in-memory-only metrics store — are addressed above. What's still genuinely open:

- **Real historical training data.** `train_isolation_forest.py` still trains on synthetic data. Now that `evaluate_model.py` exists, you can actually tell when this starts mattering: if the reported precision/recall stays high on synthetic-trained scoring, the synthetic distributions are a good enough proxy for now; once real traffic volume is large enough, swap `generate_synthetic_training_data()` for a query pulling real `(z_score, tx_count, avg_gas_fee, graph_risk_score, structural_novelty_score)` tuples off `:SENT` relationships in Neo4j (the same query shape `evaluate_model.py` already uses is most of the way there).
- **Alerting.** `metrics_api.py` and `/graph/wash-rings` both compute exactly what you'd want to alert on already — wiring a webhook (Slack/Discord) off a non-empty ring list would turn this from a dashboard into something that actually pages someone.
- **A supervised model, once you have real labels.** `evaluate_model.py` currently checks the *unsupervised* pipeline against the generator's synthetic ground truth. If this ever ingests real flagged/confirmed fraud cases, that's exactly the labeled data a supervised classifier (e.g. gradient-boosted trees on the same feature set) would need — likely a stronger model than Isolation Forest once real labels exist, since Isolation Forest's whole value proposition is not needing them.
- **Multi-hop graph queries beyond what's here.** The cycle-detection query is capped at 3–8 hops for cost reasons; a longer-hop or approximate variant (or GDS's path-finding procedures) would catch longer, more patient wash-trading loops than the current window allows.
