import os
import sys
import pyspark
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
    lit,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeansModel

# Pipeline Modules
from broadcast_engine import join_with_profiles
from storage_layer import (
    CHECKPOINT_DIR,
    DELTA_PACKAGE,
    HIVE_WAREHOUSE_DIR,
    write_anomalies_batch,
)

# 1. Windows Native Hadoop & Derby Log Configuration
os.makedirs(r"C:\hadoop\logs", exist_ok=True)  # Prevents derby.log FileNotFoundException
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

print(
    f"PySpark {spark_version} Structured Streaming Engine Started with Delta + Kafka support..."
)

# 5. Input Transaction Schema Definition
tx_schema = StructType(
    [
        StructField("tx_id", StringType(), True),
        StructField("wallet_address", StringType(), True),
        StructField("amount_usd", DoubleType(), True),
        StructField("gas_fee", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

# 6. Read Stream from Kafka
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "defi-transactions")
    .option("startingOffsets", "latest")
    .load()
)

# 7. Parse JSON Payload
parsed_stream = (
    raw_stream.selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), tx_schema).alias("data"))
    .select(
        col("data.tx_id").alias("tx_id"),
        col("data.wallet_address").alias("wallet_address"),
        col("data.amount_usd").alias("amount_usd"),
        col("data.gas_fee").alias("gas_fee"),
        col("data.timestamp").alias("timestamp"),
    )
)

# =====================================================================
# TASK 2.1: Bounded Event-Time Watermarking & Sliding Windowing
# =====================================================================
# 10-second watermark prevents JVM OOM crashes by purging old state buffers.
# 1-minute sliding window updates every 10 seconds per wallet_address.
windowed_stats = (
    parsed_stream.withWatermark("timestamp", "10 seconds")
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

# =====================================================================
# TASK 2.2: Complex Event Processing (CEP) Math Implementation
# =====================================================================
# Calculate dynamic Z-Score: Z_t = (X_t - mu_w) / sigma_w
# If stddev is null or 0 (single tx), fallback to 1.0 to avoid division by zero.
cep_stream = (
    windowed_stats.withColumn(
        "stddev_safe",
        when(
            (col("stddev_amount").isNull()) | (col("stddev_amount") == 0), 1.0
        ).otherwise(col("stddev_amount")),
    )
    .withColumn(
        "z_score",
        (col("mean_amount") - col("mean_amount")) / col("stddev_safe"),
    )
    .withColumn(
        "anomaly_reason",
        when(spark_abs(col("z_score")) > 3.0, "DYNAMIC_Z_SCORE_SPIKE")
        .when(col("tx_count") > 8, "BOT_BURST_HIGH_FREQUENCY")
        .otherwise(None),
    )
)

# =====================================================================
# TASK 2.4: In-Memory Broadcast Hash Join
# =====================================================================
# Enriches the aggregated window stream with static wallet user profile metadata.
enriched_stream = join_with_profiles(cep_stream, spark)

# =====================================================================
# TASK 2.3: MLlib Feature Vector & Inline Inference (Micro-Batch Function)
# =====================================================================
assembler = VectorAssembler(
    inputCols=["z_score", "tx_count", "avg_gas_fee"],
    outputCol="features",
    handleInvalid="skip",
)

# Load the offline pre-trained KMeans model if present
model_path = "kmeans_model"
kmeans_model = None
if os.path.exists(model_path):
    try:
        kmeans_model = KMeansModel.load(model_path)
        print(f"Loaded KMeans model successfully from '{model_path}'.")
    except Exception as e:
        print(f"Warning: Could not load KMeans model from '{model_path}': {e}")


def process_micro_batch(micro_batch_df, batch_id):
    if micro_batch_df.isEmpty():
        return

    # 1. Assemble real-time feature vector [Z_t, C_w, G_t]
    assembled_batch = assembler.transform(micro_batch_df)

    # 2. Run MLlib inference if model is loaded
    if kmeans_model is not None:
        predicted_batch = kmeans_model.transform(assembled_batch)
    else:
        predicted_batch = assembled_batch.withColumn("prediction", lit(-1))

    # 3. Filter for flagged anomalies (CEP rules or ML cluster flags)
    anomalies_df = predicted_batch.filter(
        col("anomaly_reason").isNotNull() | (col("prediction") == 1)
    )

    # 4. Append detected anomalies directly to Delta Lake storage
    if not anomalies_df.isEmpty():
        write_anomalies_batch(anomalies_df, batch_id)


# =====================================================================
# TASK 1.3 / OUTPUT SINK: Write to Delta Lake with Checkpointing
# =====================================================================
query = (
    enriched_stream.writeStream.outputMode("update")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .foreachBatch(process_micro_batch)
    .start()
)

query.awaitTermination()