"""
Storage layer, v2: Neo4j instead of Hive + Delta Lake.

Why this replaced storage_layer.py:
  - Delta Lake gave us an ACID *table* of flat anomaly rows. It could not
    represent the thing we actually care about for fraud detection: the
    relationships *between* wallets (who sent money to whom, how often,
    in what cycles). Answering "is this wallet part of a wash-trading
    ring?" against a flat table means self-joining a windowed Spark
    DataFrame N times for an N-hop cycle — expensive and awkward.
  - Neo4j stores exactly that relationship structure natively, and its
    Graph Data Science (GDS) library ships production-grade
    implementations of PageRank, Louvain community detection, and cycle
    detection that would otherwise have to be hand-rolled in Spark.

Graph model:
    (:Wallet {address, first_seen_ts, historical_tx_count,
              historical_risk_tier, pagerank, community_id,
              in_wash_ring, wash_ring_id})
        -[:SENT {tx_id, amount_usd, gas_fee, timestamp, z_score,
                 anomaly_reason, ml_score, ml_anomaly}]->
    (:Wallet)

Kept from storage_layer.py: the shared CEP thresholds and the Spark
session builder pattern (so streaming_engine.py doesn't have to guess at
Kafka connector versions).
"""
import os
import sys

from dotenv import load_dotenv
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
import pyspark

# Secrets are loaded further below, once PROJECT_ROOT is known (see the
# explicit load_dotenv() call after PROJECT_ROOT is computed).

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# graph_storage.py lives in <project root>/pipeline/ — everything that
# isn't source code (models/, data/, runtime state) lives one level up
# from here, in its own folder, so this is the one place that mapping is
# defined. Every other module imports PROJECT_ROOT/MODELS_DIR/DATA_DIR/
# RUNTIME_DIR from here rather than recomputing its own relative path,
# so moving a folder only ever requires editing this file.
PROJECT_ROOT = os.path.dirname(BASE_DIR)
MODELS_DIR = os.path.join(PROJECT_ROOT, "models")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RUNTIME_DIR = os.path.join(PROJECT_ROOT, "runtime")
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RUNTIME_DIR, exist_ok=True)

# Explicit path rather than relying on load_dotenv()'s upward-search
# default — that default does happen to work here too (it walks up from
# this file's own directory, which finds the root .env either way), but
# being explicit means it can't silently break if graph_storage.py ever
# moves again.
load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env"))

os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
    os.environ["PATH"] += r";C:\hadoop\bin"
os.makedirs(r"C:\hadoop\logs", exist_ok=True)

# Force PySpark's Python workers to use the exact active interpreter (see
# the original storage_layer.py for why: the Windows "App Execution
# Alias" stub for `python` otherwise silently hangs Spark's worker
# subprocess until it times out).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# --- Kafka connector, derived from the installed pyspark version so this
# and streaming_engine.py can never disagree on the jar to pull. ---------
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"

# --- Neo4j connection ----------------------------------------------------
# URI/user aren't secrets and are safe to default (overridable via env vars
# so the same code works against docker-compose's `neo4j` service from
# inside another container, or `localhost` from a script run on the host).
# The password is NOT defaulted here on purpose — a hardcoded fallback
# password is exactly the kind of thing that quietly ends up committed and
# then copy-pasted into a dozen other projects. Set it in a local .env
# file (copy .env.example -> .env) or export it before running anything;
# this fails loudly instead of silently connecting with a guessable
# default.
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
if not NEO4J_PASSWORD:
    raise RuntimeError(
        "NEO4J_PASSWORD is not set. Copy .env.example to .env and set a "
        "password (it must match docker-compose.yml's NEO4J_AUTH, which "
        "also reads from .env), or export NEO4J_PASSWORD directly."
    )

# --- Model artifact paths (Isolation Forest — see train_isolation_forest.py) ---
ISOFOREST_MODEL_PATH = os.path.join(MODELS_DIR, "isoforest_model.joblib")
ISOFOREST_META_PATH = os.path.join(MODELS_DIR, "isoforest_model_meta.json")

# --- Shared CEP thresholds (streaming_engine.py) -----------------------
# Unchanged from storage_layer.py — the rule-based layer didn't need to
# change just because the storage backend did.
ANOMALY_Z_THRESHOLD = 3.0
MIN_SAMPLES_FOR_ZSCORE = 3
LARGE_TX_THRESHOLD_USD = 100_000.0
BOT_BURST_TX_COUNT_THRESHOLD = 8


def get_spark_session(app_name="SentinelDeFi-Storage"):
    """Kafka-only Spark session now — no Hive support, no warehouse dir,
    no Delta package/extensions/catalog config, since nothing in the
    pipeline writes to a Spark-managed table anymore."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .getOrCreate()
    )


_driver = None


def get_neo4j_driver():
    """Process-wide singleton driver. Safe to call repeatedly — the
    underlying driver pools its own connections, so callers (streaming
    engine's foreachBatch, graph_analytics.py, metrics_api.py) should each
    just call this rather than passing a driver instance around."""
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return _driver


def init_graph_schema(driver=None):
    """Creates the uniqueness constraint backing every MERGE (Wallet
    {address: ...}) elsewhere in the codebase, plus an index used by
    graph_analytics.py's ring/community lookups. Idempotent — safe to
    call on every startup."""
    driver = driver or get_neo4j_driver()
    with driver.session() as session:
        session.run(
            "CREATE CONSTRAINT wallet_address_unique IF NOT EXISTS "
            "FOR (w:Wallet) REQUIRE w.address IS UNIQUE"
        )
        session.run(
            "CREATE INDEX wallet_community_idx IF NOT EXISTS "
            "FOR (w:Wallet) ON (w.community_id)"
        )
        session.run(
            "CREATE INDEX sent_tx_id_idx IF NOT EXISTS "
            "FOR ()-[r:SENT]-() ON (r.tx_id)"
        )
    print(f"Neo4j schema ready at {NEO4J_URI}.")


def write_transactions_batch(rows, driver=None, batch_id=None):
    """Writes one Spark micro-batch (already collected to the driver as a
    list of dicts by streaming_engine.py) into the graph.

    Two things this does that the first version didn't:

    1. Idempotent writes. `foreachBatch` gives no exactly-once guarantee
       to an arbitrary sink — Spark can and does retry a micro-batch on
       failure, which used to mean the same edge got CREATEd twice,
       silently inflating tx_count/amount_usd and corrupting every GDS
       algorithm downstream (PageRank, community sizes, cycle counts all
       assume one edge = one real aggregate). The relationship is now
       MERGEd on `window_start` (in the match pattern, alongside the
       sender/receiver endpoints) instead of CREATEd — window_start is
       exactly streaming_engine.py's own groupBy key
       (window, wallet_address, to_wallet), so a retried batch matches
       and overwrites the same relationship instead of duplicating it.
    2. Live wallet-profile updates. Wallet.historical_tx_count used to be
       a one-time synthetic seed from load_wallet_profiles.py that never
       changed again no matter how much real traffic flowed through.
       Real observed activity is now tracked separately as
       Wallet.live_tx_count (incremented every batch) and
       Wallet.last_seen_ts (kept at the max timestamp seen), so a
       wallet's profile actually reflects what it's done since the
       pipeline started watching, not just its synthetic backstory.
    """
    if not rows:
        return
    driver = driver or get_neo4j_driver()

    cypher = """
    UNWIND $rows AS row
    MERGE (sender:Wallet {address: row.wallet_address})
    ON CREATE SET sender.first_seen_ts = coalesce(row.first_seen_ts, row.timestamp),
                  sender.historical_tx_count = coalesce(row.historical_tx_count, 0),
                  sender.historical_risk_tier = coalesce(row.historical_risk_tier, 'unknown'),
                  sender.live_tx_count = 0
    SET sender.live_tx_count = coalesce(sender.live_tx_count, 0) + coalesce(row.tx_count, 1),
        sender.last_seen_ts = CASE
            WHEN sender.last_seen_ts IS NULL OR row.timestamp > sender.last_seen_ts
            THEN row.timestamp ELSE sender.last_seen_ts END
    MERGE (receiver:Wallet {address: row.to_wallet})
    ON CREATE SET receiver.historical_risk_tier = 'unknown', receiver.live_tx_count = 0
    MERGE (sender)-[r:SENT {window_start: row.window_start}]->(receiver)
    SET r.tx_id = row.tx_id,
        r.amount_usd = row.amount_usd,
        r.gas_fee = row.gas_fee,
        r.tx_count = row.tx_count,
        r.timestamp = row.timestamp,
        r.true_label = row.true_label,
        r.z_score = row.z_score,
        r.anomaly_reason = row.anomaly_reason,
        r.ml_score = row.ml_score,
        r.ml_anomaly = row.ml_anomaly
    """
    with driver.session() as session:
        session.run(cypher, rows=rows)

    if batch_id is not None:
        print(f"[batch {batch_id}] Wrote {len(rows)} transactions to Neo4j "
              f"(idempotent merge on window_start).")


if __name__ == "__main__":
    init_graph_schema()
    print("Graph storage layer provisioned. Safe to start streaming_engine.py now.")
