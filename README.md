# SentinelDeFi

Real-time DeFi transaction anomaly detection pipeline. Synthetic wallet-to-wallet
transactions stream through Kafka into a Spark Structured Streaming job that combines
rule-based CEP checks, an Isolation Forest model, and graph analytics on a Neo4j-backed
transaction graph to flag wash trading, layering, flash-loan abuse, and bot-burst
activity — with a live dashboard and a ground-truth evaluation script to measure how
well any of it actually works.

## Architecture at a glance

```
transaction_generator.py --(Kafka: defi-transactions)--> streaming_engine.py
                                                                |
                                              1. windowed CEP rule engine
                                              2. broadcast_engine.py (join vs. Neo4j:
                                                 historical profile + live graph risk)
                                              3. Isolation Forest scoring
                                              4. write SENT edge to Neo4j (idempotent
                                                 MERGE on window_start)
                                              5. HTTP POST batch stats to metrics_api.py
                                                                |
                                    Neo4j graph (graph_storage.py)
                                    (:Wallet)-[:SENT]->(:Wallet)
                                       |                              |
                          graph_analytics.py (periodic job,      metrics_api.py (FastAPI,
                          GDS: PageRank, betweenness,             persists to
                          Louvain, FastRP, cycle detection)       metrics_state.json)
                          writes results back onto Wallet nodes        |        |
                                       |                          Grafana   graph_dashboard.html
                          evaluate_model.py — reads true_label
                          back out of SENT edges, scores
                          precision/recall/F1
```

`streaming_engine.py` is the core of the pipeline: it reads from Kafka, computes a
per-(wallet, counterparty) sliding-window outlier statistic (CEP rules), joins in each
sender's historical profile *and* live graph risk score from Neo4j via
`broadcast_engine.py`, scores the enriched result with an Isolation Forest, writes each
result as a graph edge to Neo4j, and pushes each micro-batch's stats to `metrics_api.py`
over HTTP.

`graph_analytics.py` runs separately and periodically (not per micro-batch): it projects
the transaction graph into Neo4j's Graph Data Science (GDS) library, runs PageRank,
betweenness centrality, Louvain community detection, and FastRP embeddings, then runs a
plain-Cypher cycle-detection query to flag wallets sitting on a wash-trading ring —
writing all of it back onto `Wallet` nodes so the next streaming batch's broadcast join
can use it as ML features.

## Why Neo4j + Isolation Forest

The project originally stored flat anomaly rows in Delta Lake and scored them with
KMeans. Two things drove the rearchitecture:

- **Flat tables can't represent relationships.** Wash trading is a *graph* pattern —
  funds passed in a circle across several wallets. Neo4j stores wallets and
  transactions as an actual graph, and GDS ships production-grade PageRank, betweenness,
  Louvain, FastRP, and (via Cypher) cycle detection — the exact primitives fraud
  detection needs.
- **KMeans's hard cluster assignment was a poor model of "anomalous."** It required
  guessing which cluster was the anomalous one, which breaks under a coordinated attack
  and gives no sense of *how* anomalous a point is. Isolation Forest gives every
  transaction a continuous anomaly score and makes no assumption that anomalies form a
  convex cluster.

## Components

| File | Role |
|---|---|
| `transaction_generator.py` | Kafka producer. Emits synthetic transactions (`wallet_address`, `to_wallet`, `amount_usd`, `gas_fee`, `true_label`, `timestamp`) to the `defi-transactions` topic, including a hard-coded 4-wallet wash-trading ring and normal traffic that settles mostly on 3 simulated DEX/lending contracts. |
| `streaming_engine.py` | Spark Structured Streaming job. Parses Kafka JSON, computes 1-minute sliding-window CEP statistics, calls `broadcast_engine.py` for the join, scores with Isolation Forest, writes to Neo4j, and posts metrics to `metrics_api.py`. |
| `broadcast_engine.py` | Broadcast hash join against a small per-wallet context table (historical profile + live graph risk), cached from Neo4j with a 30-second TTL. |
| `graph_storage.py` | Neo4j storage layer: connection/schema setup, CEP thresholds, and `write_transactions_batch()`, which idempotently `MERGE`s `SENT` edges keyed on `window_start`. |
| `graph_analytics.py` | Periodic GDS job: PageRank, betweenness centrality, Louvain communities, FastRP embeddings, and Cypher-based wash-ring cycle detection (3–8 hop directed cycles over a deduplicated `TRANSACTED_WITH` projection). |
| `train_isolation_forest.py` | Offline trainer. Fits an `IsolationForest` (contamination = 0.02) on synthetic data matching the live feature distributions (`z_score`, `tx_count`, `avg_gas_fee`, `graph_risk_score`, `structural_novelty_score`) and saves the model + metadata for `streaming_engine.py` to load. |
| `generate_wallet_profiles.py` / `load_wallet_profiles.py` | Generate a synthetic historical wallet baseline (age, tx count, risk tier) and load it into Neo4j as `Wallet` node properties. |
| `metrics_api.py` | FastAPI service. Accepts batch metrics from `streaming_engine.py` (`POST /metrics/update`, persisted to `metrics_state.json`), and exposes read-only Neo4j-backed endpoints (`/graph/wash-rings`, `/graph/top-risk-wallets`, `/graph/recent-edges`, `/graph/summary`) for Grafana and the live dashboard. |
| `evaluate_model.py` | Reads labeled `SENT` edges back out of Neo4j and scores rule-based, ML-only, and combined detection against `true_label` (precision/recall/F1 + confusion matrix). |
| `dashboard/graph_dashboard.html` | Live force-directed graph view, polling `metrics_api.py` directly from the browser. |
| `inspect_data.py` | Standalone Neo4j reader for ad-hoc inspection. |

## Prerequisites

- **JDK 17 or 21** on `PATH` (required by PySpark).
- **Hadoop binaries** (`winutils.exe`, `hadoop.dll`) for PySpark on Windows — Spark still
  writes local streaming checkpoints even though the sink is Neo4j.
- **Docker Desktop**, running — Kafka, Zookeeper, Neo4j, and Grafana all run as
  containers.
- **Python** with `pip install -r requirements.txt`. Note: the Spark↔Kafka connector
  jar is resolved automatically by Ivy at runtime, not via pip.

## Setup

1. Copy `.env.example` to `.env` and set a real `NEO4J_PASSWORD`. Both
   `docker-compose.yml` and every Python script (via `python-dotenv`) read this same
   file, so they can't drift out of sync.
2. `pip install -r requirements.txt`

## Running the pipeline

This is the order `launch.ps1` follows:

1. `python pipeline/train_isolation_forest.py` — train the model once, before anything
   is streaming.
2. `docker-compose up -d` — brings up Zookeeper, Kafka, Neo4j (with the GDS plugin), and
   Grafana. Wait for Kafka and Neo4j to report healthy.
3. `python pipeline/generate_wallet_profiles.py` then `python pipeline/load_wallet_profiles.py`
   — creates and loads the synthetic wallet baseline into Neo4j.
4. `python pipeline/broadcast_engine.py` — optional sanity check that the broadcast join
   can reach Neo4j before starting the stream.
5. `python pipeline/graph_analytics.py` — run once immediately, then run again every
   ~2 minutes in a loop (a fresh terminal running it in a `while` loop works fine) so
   wallet risk scores stay current.
6. `python -m uvicorn metrics_api:app --host 0.0.0.0 --port 8000` — start the metrics API
   in its own terminal.
7. `python pipeline/transaction_generator.py` — start emitting synthetic transactions.
8. `python pipeline/streaming_engine.py` — start the streaming job.

Once running:

- Metrics API: `http://localhost:8000/metrics/summary`
- Graph summary: `http://localhost:8000/graph/summary`
- Wash rings: `http://localhost:8000/graph/wash-rings`
- Top risk wallets: `http://localhost:8000/graph/top-risk-wallets`
- Live dashboard: open `dashboard/graph_dashboard.html` directly in a browser
- Neo4j Browser: `http://localhost:7474` (credentials in `.env`)
- Grafana: `http://localhost:3000` (`admin` / `admin`), with the Infinity datasource
  plugin pointed at the metrics API endpoints above

## Evaluating detection accuracy

After the pipeline has been running for a few minutes:

```
python pipeline/evaluate_model.py
python pipeline/evaluate_model.py --limit 5000
```

This reads labeled `SENT` edges back out of Neo4j and reports precision/recall/F1 and a
confusion matrix for three detectors: rule-based CEP only, Isolation Forest only, and
the combined behavior that actually ships (either one firing). A full JSON report is
written to `runtime/eval_report.json`.

## Known limitations / open items

- `train_isolation_forest.py` still trains on synthetic data. Once real traffic volume
  is large enough, swap the synthetic generator for a query pulling real
  `(z_score, tx_count, avg_gas_fee, graph_risk_score, structural_novelty_score)` tuples
  off `SENT` relationships in Neo4j.
- No alerting — `metrics_api.py` and `/graph/wash-rings` already compute what you'd want
  to alert on, but nothing currently pages anyone off a non-empty ring list.
- Isolation Forest is unsupervised by necessity (no real labels yet). If real
  flagged/confirmed fraud cases start flowing in, a supervised model (e.g.
  gradient-boosted trees) on the same feature set would likely outperform it.
- Wash-ring cycle detection is capped at 3–8 hops for cost reasons; longer or
  approximate wash-trading loops (or GDS's path-finding procedures) aren't caught.