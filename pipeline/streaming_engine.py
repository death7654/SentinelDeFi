import json
import os
import sys

import numpy as np
import pyspark
import requests  # Pushes micro-batch metrics to metrics_api.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    coalesce,
    col,
    count,
    current_timestamp,
    first,
    from_json,
    stddev,
    sum as spark_sum,
    max as spark_max,
    when,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Pipeline Modules
from broadcast_engine import join_with_profiles
from graph_storage import (
    ANOMALY_Z_THRESHOLD,
    BOT_BURST_TX_COUNT_THRESHOLD,
    ISOFOREST_META_PATH,
    ISOFOREST_MODEL_PATH,
    KAFKA_PACKAGE,
    LARGE_TX_THRESHOLD_USD,
    MIN_SAMPLES_FOR_ZSCORE,
    RUNTIME_DIR,
    get_neo4j_driver,
    write_transactions_batch,
)

# 1. Windows Native Hadoop Configuration (kept only because winutils is
# still needed for Spark's own local shuffle/temp file handling on
# Windows — Hive/Delta support itself is gone)
os.makedirs(r"C:\hadoop\logs", exist_ok=True)
os.environ["HADOOP_HOME"] = r"C:\hadoop"
if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
    os.environ["PATH"] += r";C:\hadoop\bin"

# 2. Force PySpark to use the exact active Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 3. Spark Session — Kafka only now. No warehouse dir, no Delta package,
# no Hive support, no Derby log config: none of that exists anymore now
# that the sink is Neo4j instead of a Spark-managed table.
spark = (
    SparkSession.builder.appName("SentinelDeFi-Streaming")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.master", "local[*]")
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"PySpark {pyspark.__version__} Streaming Engine Started (Neo4j sink)...")

# 4. Schema Definition & Kafka Reader
# `to_wallet` is new — without a counterparty, transactions had no graph
# edge to write at all. See transaction_generator.py.
tx_schema = StructType([
    StructField("tx_id", StringType(), True),
    StructField("wallet_address", StringType(), True),
    StructField("to_wallet", StringType(), True),
    StructField("amount_usd", DoubleType(), True),
    StructField("gas_fee", DoubleType(), True),
    StructField("true_label", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "defi-transactions")
    .option("startingOffsets", "latest")
    .load()
)

# 5. Parse JSON Payload
parsed_stream = (
    raw_stream.selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), tx_schema).alias("data"))
    .select(
        col("data.tx_id").alias("tx_id"),
        col("data.wallet_address").alias("wallet_address"),
        col("data.to_wallet").alias("to_wallet"),
        col("data.amount_usd").alias("amount_usd"),
        col("data.gas_fee").alias("gas_fee"),
        col("data.true_label").alias("true_label"),
        coalesce(col("data.timestamp"), current_timestamp()).alias("timestamp"),
    )
)

# 6. Watermarking & Sliding Window Aggregation
#
# Grouped by (window, wallet_address, to_wallet) now, not just (window,
# wallet_address) — one aggregate row per *wallet pair* per window. This
# is what lets each aggregate map directly onto one graph edge
# (sender -> receiver) instead of losing the counterparty entirely, the
# way the original per-wallet aggregation did. It's also a more useful
# CEP unit: "this wallet burst-transacted against this specific
# counterparty" is a stronger signal than "this wallet was busy" alone.
windowed_stats = (
    parsed_stream.withWatermark("timestamp", "1 minute")
    .groupBy(
        window(col("timestamp"), "1 minute", "10 seconds"),
        col("wallet_address"),
        col("to_wallet"),
    )
    .agg(
        avg("amount_usd").alias("mean_amount"),
        stddev("amount_usd").alias("stddev_amount"),
        spark_max("amount_usd").alias("max_amount"),
        spark_sum("amount_usd").alias("total_amount_usd"),
        count("tx_id").alias("tx_count"),
        avg("gas_fee").alias("avg_gas_fee"),
        first("tx_id").alias("sample_tx_id"),
        # true_label is homogeneous within a (wallet, counterparty, window)
        # group for every case the generator produces (a wash-trade hop
        # and a bot-burst run each only ever hit one counterparty per
        # wallet), so first() is exact here, not just a convenient
        # approximation — see evaluate_model.py, which relies on this.
        first("true_label").alias("true_label"),
        spark_max("timestamp").alias("last_seen_ts"),
    )
    .withColumn("window_start", col("window.start").cast("string"))
)

# 7. Complex Event Processing (CEP) Anomaly Logic
#
# z_score is a Grubbs'-test-style outlier statistic — (max_amount -
# mean_amount) / stddev_amount for this (wallet, counterparty, window)
# triple — only statistically meaningful with >=3 samples, gated on
# MIN_SAMPLES_FOR_ZSCORE. See graph_storage.py for the threshold values
# (unchanged from the original CEP design).
cep_stream = (
    windowed_stats.withColumn(
        "stddev_safe",
        when(
            (col("stddev_amount").isNull()) | (col("stddev_amount") == 0), 1.0
        ).otherwise(col("stddev_amount")),
    )
    .withColumn(
        "z_score",
        (col("max_amount") - col("mean_amount")) / col("stddev_safe"),
    )
    .withColumn(
        "rule_reason",
        when(col("max_amount") > LARGE_TX_THRESHOLD_USD, "LARGE_SINGLE_TRANSACTION")
        .when(
            (col("tx_count") >= MIN_SAMPLES_FOR_ZSCORE)
            & (spark_abs(col("z_score")) > ANOMALY_Z_THRESHOLD),
            "DYNAMIC_Z_SCORE_SPIKE",
        )
        .when(col("tx_count") > BOT_BURST_TX_COUNT_THRESHOLD, "BOT_BURST_HIGH_FREQUENCY")
        .otherwise(None),
    )
)

# 8. Broadcast Join — pulls in historical wallet profile AND live graph
# risk (PageRank / community / wash-ring flag from graph_analytics.py),
# keyed on the sender (wallet_address). See broadcast_engine.py.
enriched_stream = join_with_profiles(cep_stream, spark)

# 9. ML Anomaly Signal (Isolation Forest — see train_isolation_forest.py)
#
# Loaded once at startup, same graceful-degradation behavior as the
# original KMeans setup: if train_isolation_forest.py hasn't been run
# yet, ml_score/ml_anomaly are just always None/False and the pipeline
# falls back to rule-based detection only.
ml_model = None
ml_feature_cols = None
ml_threshold = 0.0
try:
    import joblib

    ml_model = joblib.load(ISOFOREST_MODEL_PATH)
    with open(ISOFOREST_META_PATH) as f:
        meta = json.load(f)
    ml_feature_cols = meta["feature_cols"]
    ml_threshold = meta["anomaly_score_threshold"]
    print(f"Loaded Isolation Forest from {ISOFOREST_MODEL_PATH} "
          f"(features={ml_feature_cols}, threshold={ml_threshold})")
except Exception as e:
    print(
        f"[WARN] Could not load Isolation Forest model/metadata from "
        f"{ISOFOREST_MODEL_PATH} ({e}). Run train_isolation_forest.py first "
        f"for ML-assisted detection. Continuing with rule-based detection only."
    )

neo4j_driver = get_neo4j_driver()

API_URL = "http://localhost:8000/metrics/update"


def sanitize_value(val):
    """Recursively converts datetimes, timestamps, and nested dicts/rows
    to JSON-safe formats."""
    if hasattr(val, "isoformat"):
        return val.isoformat()
    elif isinstance(val, dict):
        return {k: sanitize_value(v) for k, v in val.items()}
    elif isinstance(val, list):
        return [sanitize_value(v) for v in val]
    return val


def score_with_isolation_forest(batch_data):
    """Scores every row in the batch with the Isolation Forest in one
    vectorized call rather than row-by-row — cheap at these batch sizes,
    and the natural way sklearn wants its input anyway. Mutates each row
    dict in place, adding ml_score / ml_anomaly, and folds the verdict
    into anomaly_reason when no rule already fired (rule-based reasons
    still take priority, same precedence as the original KMeans setup)."""
    if ml_model is None:
        for row in batch_data:
            row["ml_score"] = None
            row["ml_anomaly"] = False
            row["anomaly_reason"] = row.get("rule_reason")
        return

    X = np.array([
        [
            row.get("z_score") or 0.0,
            row.get("tx_count") or 0,
            row.get("avg_gas_fee") or 0.0,
            row.get("graph_risk_score") or 0.0,
            row.get("structural_novelty_score") or 0.0,
        ]
        for row in batch_data
    ])
    scores = ml_model.decision_function(X)

    for row, score in zip(batch_data, scores):
        row["ml_score"] = float(score)
        row["ml_anomaly"] = bool(score < ml_threshold)
        row["anomaly_reason"] = row.get("rule_reason") or (
            "ML_ISOLATION_FOREST_ANOMALY" if row["ml_anomaly"] else None
        )


def process_micro_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    records = batch_df.collect()
    batch_data = []
    for row in records:
        r = row.asDict(recursive=True)
        r_clean = {k: sanitize_value(v) for k, v in r.items()}
        batch_data.append(r_clean)

    score_with_isolation_forest(batch_data)

    # Shape each row for graph_storage.write_transactions_batch: one row
    # per (sender, receiver, window) aggregate = one SENT edge.
    graph_rows = [
        {
            "tx_id": r.get("sample_tx_id"),
            "window_start": r.get("window_start"),
            "wallet_address": r["wallet_address"],
            "to_wallet": r["to_wallet"],
            "amount_usd": r.get("total_amount_usd"),
            "gas_fee": r.get("avg_gas_fee"),
            "tx_count": r.get("tx_count"),
            "timestamp": r.get("last_seen_ts"),
            "first_seen_ts": r.get("first_seen_ts"),
            "historical_tx_count": r.get("historical_tx_count"),
            "historical_risk_tier": r.get("historical_risk_tier"),
            "true_label": r.get("true_label"),
            "z_score": r.get("z_score"),
            "anomaly_reason": r.get("anomaly_reason"),
            "ml_score": r.get("ml_score"),
            "ml_anomaly": r.get("ml_anomaly"),
        }
        for r in batch_data
    ]

    try:
        write_transactions_batch(graph_rows, driver=neo4j_driver, batch_id=batch_id)
    except Exception as e:
        print(f"Error persisting batch {batch_id} to Neo4j: {e}")

    # Compute batch statistics for the metrics API / Grafana feed.
    batch_count = len(batch_data)
    anomalies = [r for r in batch_data if r.get("anomaly_reason")]
    anomaly_count = len(anomalies)
    avg_z = (
        sum(r.get("z_score", 0.0) or 0.0 for r in batch_data) / batch_count
        if batch_count > 0
        else 0.0
    )

    payload = {
        "status": "active",
        "batch_id": batch_id,
        "processed_delta": batch_count,
        "anomaly_delta": anomaly_count,
        "avg_z_score": round(avg_z, 2),
        "recent_records": batch_data[:50],
    }

    try:
        response = requests.post(API_URL, json=payload, timeout=2)
        if response.status_code != 200:
            print(f"API returned status {response.status_code} for batch {batch_id}")
    except Exception as e:
        print(f"Failed pushing metrics for batch {batch_id}: {e}")


# Start Structured Streaming Sink
query = (
    enriched_stream.writeStream.outputMode("update")
    .option("checkpointLocation", os.path.join(RUNTIME_DIR, "checkpoints", "neo4j_sink"))
    .foreachBatch(process_micro_batch)
    .start()
)

query.awaitTermination()
