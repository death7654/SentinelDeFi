from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

import os
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
local_csv_path = os.path.join(BASE_DIR, "wallet_profiles.csv")
WALLET_PROFILES_CSV = Path(local_csv_path).as_uri()

os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
    os.environ["PATH"] += r";C:\hadoop\bin"


DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"  # must match get_spark_session()'s Scala version below
DELTA_TABLE_PATH = "file:///C:/sentineldefi/delta/anomalies"
HIVE_WAREHOUSE_DIR = "file:///C:/sentineldefi/hive-warehouse"
CHECKPOINT_DIR = "file:///C:/sentineldefi/checkpoints/anomalies"

# Schema for records written into the Delta anomaly table. This matches the
# enriched_stream produced by streaming_engine.py: raw transaction fields,
# plus the wallet profile fields added by the broadcast_engine.py join
# (first_seen_ts, historical_tx_count, historical_risk_tier), plus z_score
# and anomaly_reason from the CEP layer. Keep this in sync with whatever
# columns enriched_stream actually carries — a mismatch here will make a
# freshly-provisioned Delta table reject the first streaming append.
ANOMALY_SCHEMA = StructType([
    StructField("wallet_address", StringType(), True),
    StructField("tx_id", StringType(), True),
    StructField("amount_usd", DoubleType(), True),
    StructField("gas_fee", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("first_seen_ts", TimestampType(), True),
    StructField("historical_tx_count", IntegerType(), True),
    StructField("historical_risk_tier", StringType(), True),
    StructField("z_score", DoubleType(), True),
    StructField("anomaly_reason", StringType(), True),
])

WALLET_PROFILE_SCHEMA = StructType([
    StructField("wallet_address", StringType(), True),
    StructField("first_seen_ts", TimestampType(), True),
    StructField("historical_tx_count", IntegerType(), True),
    StructField("historical_risk_tier", StringType(), True),
])


def get_spark_session(app_name="SentinelDeFi-Storage"):
    """Shared session builder — import this from streaming_engine.py too
    so both scripts agree on Hive warehouse location and Delta config."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.warehouse.dir", HIVE_WAREHOUSE_DIR)
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions", "-Dderby.stream.error.file=C:/hadoop/logs/derby.log")
        .enableHiveSupport()
        .getOrCreate()
    )


def provision_hive_database(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS sentineldefi")
    spark.sql("USE sentineldefi")


def register_wallet_profiles_table(spark):
    """
    Loads the synthetic historical profile CSV (generate_wallet_profiles.py)
    and registers it as a managed Hive table. This is the small
    (well under 10MB) lookup table the Broadcast Hash Join reads at
    startup — see proposal section 4.5 on why broadcast only works at
    this scale.
    """
    df = (
        spark.read.option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ssXXX")
        .schema(WALLET_PROFILE_SCHEMA)
        .csv(WALLET_PROFILES_CSV)
    )
    df.write.mode("overwrite").saveAsTable("sentineldefi.wallet_profiles")
    print(f"Registered sentineldefi.wallet_profiles ({df.count()} rows) in Hive.")


def provision_delta_anomaly_table(spark):
    """
    Creates the ACID anomaly table backed by Delta Lake, and registers
    it in the Hive metastore so Grafana or any SQL client can query it
    as sentineldefi.anomalies without knowing the underlying file path.
    """
    empty_df = spark.createDataFrame([], ANOMALY_SCHEMA)
    (
        empty_df.write.format("delta")
        .mode("ignore")  # no-op if the table/path already exists
        .save(DELTA_TABLE_PATH)
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS sentineldefi.anomalies
        USING DELTA
        LOCATION '{DELTA_TABLE_PATH}'
    """)
    print(f"Delta anomaly table ready at {DELTA_TABLE_PATH}, registered as sentineldefi.anomalies.")


def write_anomalies_batch(anomalies_df, batch_id):
  """Appends each micro-batch to the Delta table with automatic schema evolution."""
  if anomalies_df.isEmpty():  # Native PySpark check (faster than .rdd.isEmpty())
    return

  (
      anomalies_df.write.format("delta")
      .mode("append")
      .option(
          "mergeSchema", "true"
      )  # Resolves [_LEGACY_ERROR_TEMP_DELTA_0007] schema mismatch errors
      .save(DELTA_TABLE_PATH)
  )

  print(f"[batch {batch_id}] Wrote anomalies to Delta at {DELTA_TABLE_PATH}.")


if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    provision_hive_database(spark)
    register_wallet_profiles_table(spark)
    provision_delta_anomaly_table(spark)
    print("Storage layer provisioned. Safe to start streaming_engine.py now.")