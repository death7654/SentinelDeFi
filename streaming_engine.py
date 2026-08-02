import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# 1. Windows Native Hadoop Configuration
# Explicitly set native environment variables so PySpark locates winutils & hadoop.dll
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

# 2. Dynamic Dependency Injection & Session Builder
spark_version = pyspark.__version__

# PySpark 4.x uses Scala 2.13 by default (_2.13)
# Dynamically pull the exact artifact version matching the installed PySpark release
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}"

spark = (
    SparkSession.builder.appName("SentinelDeFi-Streaming")
    .config("spark.jars.packages", kafka_package)
    .getOrCreate()
)

# Suppress noisy INFO log messages in console output
spark.sparkContext.setLogLevel("WARN")

print(
    f"PySpark {spark_version} Structured Streaming Engine Started with {kafka_package}..."
)

# 3. Schema Definition for DeFi Transactions
# Matches the JSON payload emitted by the Kafka producer
tx_schema = StructType(
    [
        StructField("tx_id", StringType(), True),
        StructField("wallet_address", StringType(), True),
        StructField("amount_usd", DoubleType(), True),
        StructField("gas_fee", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

# 4. Read Stream from Kafka
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "defi-transactions")
    .option("startingOffsets", "latest")
    .load()
)

# 5. Transformation Pipeline
# Kafka sends raw binary payload in the 'value' column.
# Cast binary -> String -> parse JSON according to defined Schema.
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

# 6. Write Stream to Console (Micro-batch Sink)
query = (
    parsed_stream.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

# Keep the streaming query active until interrupted (Ctrl + C)
query.awaitTermination()