"""
Member 1 — Week 7 deliverable: Broadcast Hash Join.

Joins the live transaction stream against the small, in-memory-cached
wallet_profiles Hive table (or CSV fallback) so downstream CEP/ML logic
can factor in wallet age and historical risk tier without a per-record disk read.
"""
import os
from pathlib import Path
from pyspark.sql.functions import broadcast
from storage_layer import get_spark_session, WALLET_PROFILE_SCHEMA


def load_wallet_profiles(spark):
    """
    Attempts to read from the sentineldefi.wallet_profiles Hive table.
    If the table has not been registered in Hive yet, falls back directly
    to reading wallet_profiles.csv from disk.
    """
    try:
        return spark.table("sentineldefi.wallet_profiles")
    except Exception:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        local_csv = os.path.join(base_dir, "wallet_profiles.csv")
        staged_csv = r"C:\sentineldefi\wallet_profiles.csv"

        csv_path = local_csv if os.path.exists(local_csv) else staged_csv
        csv_uri = Path(csv_path).as_uri()

        print(
            f"[WARN] Hive table 'sentineldefi.wallet_profiles' not found. "
            f"Loading directly from CSV: {csv_uri}"
        )
        return (
            spark.read.option("header", True)
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ssXXX")
            .schema(WALLET_PROFILE_SCHEMA)
            .csv(csv_uri)
        )


def join_with_profiles(stream_df, spark):
    profiles = load_wallet_profiles(spark)
    return stream_df.join(
        broadcast(profiles),
        on="wallet_address",
        how="left",
    )


if __name__ == "__main__":
    spark = get_spark_session("SentinelDeFi-BroadcastJoinTest")
    sample_stream = spark.createDataFrame(
        [("0x0000000000000000000000000000000005", 1200.0)],
        ["wallet_address", "amount_usd"],
    )
    joined = join_with_profiles(sample_stream, spark)
    joined.explain()
    joined.show(truncate=False)