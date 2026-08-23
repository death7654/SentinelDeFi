import json
import os
import sys
import pyspark
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    coalesce,
    col,
    count,
    current_timestamp,
    from_json,
    lit,
    max as spark_max,
    stddev,
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
import requests  # Pushes micro-batch metrics to metrics_api.py

# Pipeline Modules
from broadcast_engine import join_with_profiles
from storage_layer import (
    ANOMALY_Z_THRESHOLD,
    BOT_BURST_TX_COUNT_THRESHOLD,
    CHECKPOINT_DIR,
    DELTA_PACKAGE,
    HIVE_WAREHOUSE_DIR,
    KAFKA_PACKAGE,
    KMEANS_MODEL_META_PATH,
    KMEANS_MODEL_PATH,
    LARGE_TX_THRESHOLD_USD,
    MIN_SAMPLES_FOR_ZSCORE,
    write_anomalies_batch,
)

# 1. Windows Native Hadoop & Derby Log Configuration
os.makedirs(r"C:\hadoop\logs", exist_ok=True)
os.environ["HADOOP_HOME"] = r"C:\hadoop"
if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
  os.environ["PATH"] += r";C:\hadoop\bin"

# 2. Force PySpark to use the exact active Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 3. Package Dependency Resolution — KAFKA_PACKAGE/DELTA_PACKAGE come from
# storage_layer.py so this session and get_spark_session() can never load
# mismatched connector jar versions.
combined_packages = f"{DELTA_PACKAGE},{KAFKA_PACKAGE}"

# 4. Spark Session with Hive, Delta Lake, and Schema Auto-Merge Enabled
# 4. Spark Session with Hive, Delta Lake, and Dynamic Host Bindings
spark = (
    SparkSession.builder.appName("SentinelDeFi-Streaming")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.master", "local[*]")
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.sql.warehouse.dir", HIVE_WAREHOUSE_DIR)
    .config("spark.jars.packages", combined_packages)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .config(
        "spark.driver.extraJavaOptions",
        "-Dderby.stream.error.file=C:/hadoop/logs/derby.log",
    )
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"PySpark {pyspark.__version__} Streaming Engine Started...")

# 5. Schema Definition & Kafka Reader
tx_schema = StructType([
    StructField("tx_id", StringType(), True),
    StructField("wallet_address", StringType(), True),
    StructField("amount_usd", DoubleType(), True),
    StructField("gas_fee", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
])

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "defi-transactions")
    .option("startingOffsets", "latest")
    .load()
)

# 6. Parse JSON Payload
parsed_stream = (
    raw_stream.selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), tx_schema).alias("data"))
    .select(
        col("data.tx_id").alias("tx_id"),
        col("data.wallet_address").alias("wallet_address"),
        col("data.amount_usd").alias("amount_usd"),
        col("data.gas_fee").alias("gas_fee"),
        coalesce(col("data.timestamp"), current_timestamp()).alias("timestamp"),
    )
)

# 7. Watermarking & Sliding Window Aggregation
windowed_stats = (
    parsed_stream.withWatermark("timestamp", "1 minute")
    .groupBy(
        window(col("timestamp"), "1 minute", "10 seconds"),
        col("wallet_address"),
    )
    .agg(
        avg("amount_usd").alias("mean_amount"),
        stddev("amount_usd").alias("stddev_amount"),
        spark_max("amount_usd").alias("max_amount"),
        count("tx_id").alias("tx_count"),
        avg("gas_fee").alias("avg_gas_fee"),
    )
)

# 8. Complex Event Processing (CEP) Anomaly Logic
#
# NOTE on z_score: this is intentionally a Grubbs'-test-style outlier
# statistic — (max_amount - mean_amount) / stddev_amount, i.e. "how many
# stddevs is the single largest transaction in this window from this
# window's own mean" — not a z-score against an external baseline (no such
# baseline exists in this pipeline; wallet_profiles has no $-amount stats
# to compare against). This is only statistically meaningful with >=3
# samples (Grubbs' test's own minimum-n requirement), which is why it's
# gated on MIN_SAMPLES_FOR_ZSCORE below.
#
# Previously this computed mean_amount / stddev_safe / 100.0, which isn't
# a z-score at all and blew up for tx_count == 1 windows (the common
# case): stddev is null there, gets clamped to 1.0, so the "z-score"
# degenerated to amount / 100 — meaning any transaction over ~$300 (which,
# with amounts drawn from Exponential(scale=250), happens ~30% of the
# time for completely normal traffic) tripped the anomaly threshold.
# That single-count case is now handled separately by the explicit
# LARGE_SINGLE_TRANSACTION rule below instead of being misdetected via a
# meaningless "z-score".
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

# 8b. ML Anomaly Signal (KMeans, trained offline by train_kmeans.py)
#
# This is a second, learned signal layered on top of the rule-based CEP
# checks above, not a replacement — it can catch windows that don't cross
# any single hardcoded threshold but still cluster with historically
# anomalous behavior across all three features (z_score, tx_count,
# avg_gas_fee) jointly. anomaly_reason only falls back to the ML verdict
# when no rule already fired, so the interpretable rule-based reasons
# still take priority for reporting.
#
# The model is optional at runtime: if train_kmeans.py hasn't been run
# yet, the pipeline degrades gracefully to rule-only detection instead of
# crashing, and ml_cluster/ml_anomaly are still present in the output
# schema (as nulls) so the Delta table schema doesn't change once someone
# does train and load a model.
ml_model = None
ml_anomaly_clusters = []
try:
    ml_model = PipelineModel.load(KMEANS_MODEL_PATH)
    with open(KMEANS_MODEL_META_PATH) as f:
        meta = json.load(f)
    ml_anomaly_clusters = meta["anomaly_clusters"]
    print(
        f"Loaded KMeans model from {KMEANS_MODEL_PATH} "
        f"(anomaly clusters = {ml_anomaly_clusters}, normal cluster = {meta['normal_cluster']})"
    )
except Exception as e:
    print(
        f"[WARN] Could not load KMeans model/metadata from {KMEANS_MODEL_PATH} ({e}). "
        f"Run train_kmeans.py first for ML-assisted detection. "
        f"Continuing with rule-based detection only."
    )

if ml_model is not None:
    ml_scored = (
        ml_model.transform(cep_stream)
        .withColumnRenamed("prediction", "ml_cluster")
        .withColumn("ml_anomaly", col("ml_cluster").isin(ml_anomaly_clusters))
        .drop("features", "scaledFeatures")
    )
else:
    ml_scored = cep_stream.withColumn("ml_cluster", lit(None).cast("int")).withColumn(
        "ml_anomaly", lit(False)
    )

cep_stream = ml_scored.withColumn(
    "anomaly_reason",
    coalesce(
        col("rule_reason"),
        when(col("ml_anomaly"), "ML_CLUSTER_ANOMALY"),
    ),
).drop("rule_reason")

# 9. In-Memory Broadcast Join
enriched_stream = join_with_profiles(cep_stream, spark)

# 10. Micro-batch Writer & API Metric Push
API_URL = "http://localhost:8000/metrics/update"


def sanitize_value(val):
  """Recursively converts datetimes, timestamps, and nested dicts to JSON-safe formats."""
  if hasattr(val, "isoformat"):
    return val.isoformat()
  elif isinstance(val, dict):
    return {k: sanitize_value(v) for k, v in val.items()}
  elif isinstance(val, list):
    return [sanitize_value(v) for v in val]
  return val


def process_micro_batch(batch_df, batch_id):
  if batch_df.isEmpty():
    return

  # Persist batch to Delta Lake / Storage Layer
  try:
    write_anomalies_batch(batch_df, batch_id)
  except Exception as e:
    print(f"Error persisting batch {batch_id} to storage: {e}")

  # Convert PySpark batch DataFrame to JSON-safe Python dictionaries
  records = batch_df.collect()
  batch_data = []
  for row in records:
    r = row.asDict(recursive=True)
    # Recursively convert all datetime objects (first_seen_ts, window, timestamp)
    r_clean = {k: sanitize_value(v) for k, v in r.items()}
    batch_data.append(r_clean)

  # Compute batch statistics
  #
  # An "anomaly" is anything the CEP layer above already tagged with an
  # anomaly_reason. Previously this also re-checked z_score >= 3.5 here,
  # a second, different threshold from the CEP layer's z_score > 3.0 —
  # anomaly_reason is already a strict superset of that check, so the
  # extra condition was redundant and just a second place for the
  # threshold to drift out of sync.
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

  # Push to FastAPI metrics backend
  try:
    response = requests.post(API_URL, json=payload, timeout=2)
    if response.status_code != 200:
      print(
          f"API returned status {response.status_code} for batch {batch_id}"
      )
  except Exception as e:
    print(f"Failed pushing metrics for batch {batch_id}: {e}")


# Start Structured Streaming Sink
query = (
    enriched_stream.writeStream.outputMode("update")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .foreachBatch(process_micro_batch)
    .start()
)

query.awaitTermination()