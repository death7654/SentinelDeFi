import os
import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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

# 2. Dynamic Package Dependency Resolution
spark_version = pyspark.__version__
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"
combined_packages = f"{DELTA_PACKAGE},{kafka_package}"

# 3. Spark Session with Hive, Delta Lake, and Kafka Support
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

# 4. Input Transaction Schema Definition
tx_schema = StructType(
    [
        StructField("tx_id", StringType(), True),
        StructField("wallet_address", StringType(), True),
        StructField("amount_usd", DoubleType(), True),
        StructField("gas_fee", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

# 5. Read Stream from Kafka
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
        col("data.timestamp").alias("timestamp"),
    )
)

# 7. Broadcast Hash Join with Wallet Profiles
enriched_stream = join_with_profiles(parsed_stream, spark)

# 8. Filter and Tag Anomaly Events
anomalies_stream = (
    enriched_stream.withColumn(
        "z_score",
        when(col("amount_usd") >= 1_000_000, 5.0)
        .when(col("gas_fee") >= 0.05, 3.5)
        .otherwise(0.0),
    )
    .withColumn(
        "anomaly_reason",
        when(col("amount_usd") >= 1_000_000, "FLASH_LOAN_SPIKE")
        .when(col("gas_fee") >= 0.05, "HIGH_GAS_BOT_BURST")
        .otherwise(None),
    )
    .filter(col("anomaly_reason").isNotNull())
)

# 9. Output Sink: Append Micro-Batches directly into Delta Lake
# checkpointLocation is required for fault-tolerant recovery: it durably
# records which Kafka offsets have been committed through foreachBatch,
# so a crash/restart resumes exactly where it left off instead of either
# reprocessing already-written anomalies or silently skipping data.
# Without it Spark falls back to a throwaway temp directory that doesn't
# survive a restart — this is what Module 3's fault-tolerance drill
# (kill a worker mid-run, prove zero data loss) actually depends on.
query = (
    anomalies_stream.writeStream.outputMode("append")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .foreachBatch(write_anomalies_batch)
    .start()
)

query.awaitTermination()