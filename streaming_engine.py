import os
import sys
import requests  # Pushes micro-batch metrics to metrics_api.py
import pyspark
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    stddev,
    when,
    window,
    abs as spark_abs,
    coalesce,
    current_timestamp,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.ml.feature import VectorAssembler

# Pipeline Modules
from broadcast_engine import join_with_profiles
from storage_layer import (
    CHECKPOINT_DIR,
    DELTA_PACKAGE,
    HIVE_WAREHOUSE_DIR,
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

# 3. Dynamic Package Dependency Resolution
spark_version = pyspark.__version__
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"
combined_packages = f"{DELTA_PACKAGE},{kafka_package}"

# 4. Spark Session with Hive, Delta Lake, and Kafka Support
spark = (
    SparkSession.builder.appName("SentinelDeFi-Streaming")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.warehouse.dir", HIVE_WAREHOUSE_DIR)
    .config("spark.jars.packages", combined_packages)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config(
        "spark.driver.extraJavaOptions",
        "-Dderby.stream.error.file=C:/hadoop/logs/derby.log",
    )
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"PySpark {spark_version} Streaming Engine Started...")

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
        count("tx_id").alias("tx_count"),
        avg("gas_fee").alias("avg_gas_fee"),
    )
)

# 8. Complex Event Processing (CEP) Z-Score Logic
cep_stream = (
    windowed_stats.withColumn(
        "stddev_safe",
        when((col("stddev_amount").isNull()) | (col("stddev_amount") == 0), 1.0)
        .otherwise(col("stddev_amount")),
    )
    .withColumn(
        "z_score",
        (col("mean_amount") / col("stddev_safe")) / 100.0,
    )
    .withColumn(
        "anomaly_reason",
        when(spark_abs(col("z_score")) > 3.0, "DYNAMIC_Z_SCORE_SPIKE")
        .when(col("tx_count") > 8, "BOT_BURST_HIGH_FREQUENCY")
        .otherwise(None),
    )
)

# 9. In-Memory Broadcast Join
enriched_stream = join_with_profiles(cep_stream, spark)

# 10. Micro-batch Writer & API Metric Push
API_URL = "http://localhost:8000/api/metrics"  # Adjust to match your metrics API route

def process_micro_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # Persist batch to Delta Lake / Storage Layer
    try:
        write_anomalies_batch(batch_df, batch_id)
    except Exception as e:
        print(f"Error persisting batch {batch_id} to storage: {e}")

    # Convert to Python dicts safely
    records = batch_df.collect()
    batch_data = []
    for row in records:
        r = row.asDict()
        # Sanitize non-serializable objects (datetime, Row) for JSON/API output
        if "timestamp" in r and r["timestamp"]:
            r["timestamp"] = r["timestamp"].isoformat()
        if "window" in r and r["window"]:
            r["window"] = {
                "start": r["window"]["start"].isoformat(),
                "end": r["window"]["end"].isoformat()
            }
        batch_data.append(r)

    # Compute batch statistics
    batch_count = len(batch_data)
    anomalies = [r for r in batch_data if (r.get("z_score") or 0.0) >= 3.5 or r.get("anomaly_reason")]
    anomaly_count = len(anomalies)
    avg_z = sum(r.get("z_score", 0.0) or 0.0 for r in batch_data) / batch_count if batch_count > 0 else 0.0

    payload = {
        "status": "active",
        "batch_id": batch_id,
        "processed_delta": batch_count,
        "anomaly_delta": anomaly_count,
        "avg_z_score": round(avg_z, 2),
        "recent_records": batch_data[:50]
    }

    # Push to API server so total_processed and anomaly_count increment
    try:
        requests.post(API_URL, json=payload, timeout=2)
    except Exception as e:
        print(f"Failed pushing metrics for batch {batch_id}: {e}")

# Start Structured Streaming Sink
query = (
    enriched_stream.writeStream
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .foreachBatch(process_micro_batch)
    .start()
)

query.awaitTermination()