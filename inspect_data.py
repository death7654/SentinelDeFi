"""
SentinelDeFi — quick data inspector.

Reads the two on-disk outputs of the pipeline directly with pandas/pyarrow,
without spinning up a SparkSession (no JVM, no Ivy resolution, no JAVA_HOME
juggling — just reads the parquet files that Spark already wrote).

Covers:
  1. sentineldefi.anomalies (Delta table) — read via the `deltalake` package
     if installed (correct: honors the _delta_log, so you only see live
     files, not orphaned parts from old batches). Falls back to a naive
     glob over part-*.parquet if `deltalake` isn't installed, which works
     fine as long as you haven't run VACUUM or had any failed/retried
     batches leave orphaned files behind.
  2. sentineldefi.wallet_profiles (plain Hive-managed parquet table, no
     transaction log) — just glob + concat.

Usage:
    pip install pandas pyarrow deltalake
    python inspect_data.py
    python inspect_data.py --anomalies-path "C:\\sentineldefi\\delta\\anomalies" ^
                            --profiles-path "C:\\sentineldefi\\hive-warehouse\\sentineldefi.db\\wallet_profiles"
"""
import argparse
import glob
import os
import sys

import pandas as pd

DEFAULT_ANOMALIES_PATH = r"C:\sentineldefi\delta\anomalies"
DEFAULT_PROFILES_PATH = r"C:\sentineldefi\hive-warehouse\sentineldefi.db\wallet_profiles"


def read_delta_anomalies(path: str) -> pd.DataFrame:
    """Prefer the `deltalake` package so reads honor _delta_log (correct
    even after compaction/vacuum/failed-batch retries). Falls back to a
    plain glob over part files if the package isn't installed."""
    try:
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        return dt.to_pandas()
    except ImportError:
        print(
            "[WARN] 'deltalake' package not installed — falling back to a naive "
            "glob over part-*.parquet. This can include stale files if you've "
            "ever run VACUUM or had a batch retry. `pip install deltalake` for "
            "a correct read.",
            file=sys.stderr,
        )
        files = glob.glob(os.path.join(path, "part-*.snappy.parquet"))
        if not files:
            raise FileNotFoundError(f"No parquet part files found under {path}")
        return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def read_hive_table(path: str) -> pd.DataFrame:
    files = glob.glob(os.path.join(path, "part-*.snappy.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet part files found under {path}")
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--anomalies-path", default=DEFAULT_ANOMALIES_PATH)
    parser.add_argument("--profiles-path", default=DEFAULT_PROFILES_PATH)
    parser.add_argument(
        "--rows", type=int, default=20, help="How many anomaly rows to print (default 20)"
    )
    args = parser.parse_args()

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)

    print("=" * 70)
    print("WALLET PROFILES  (sentineldefi.wallet_profiles)")
    print("=" * 70)
    try:
        profiles = read_hive_table(args.profiles_path)
        print(f"{len(profiles)} wallets\n")
        print(profiles.to_string(index=False))
        print(f"\nrisk tier distribution:\n{profiles['historical_risk_tier'].value_counts()}")
    except FileNotFoundError as e:
        print(f"[SKIP] {e}")

    print("\n" + "=" * 70)
    print("ANOMALIES  (sentineldefi.anomalies)")
    print("=" * 70)
    try:
        anomalies = read_delta_anomalies(args.anomalies_path)
        print(f"{len(anomalies)} anomaly rows total\n")

        cols = [
            c
            for c in [
                "tx_id",
                "wallet_address",
                "amount_usd",
                "gas_fee",
                "anomaly_reason",
                "z_score",
                "historical_risk_tier",
                "timestamp",
            ]
            if c in anomalies.columns
        ]
        print(anomalies[cols].head(args.rows).to_string(index=False))

        print(f"\nanomaly_reason counts:\n{anomalies['anomaly_reason'].value_counts()}")

        null_wallets = anomalies["wallet_address"].isnull().sum()
        print(f"\nrows with null wallet_address (broadcast join sanity check): {null_wallets}")
        if null_wallets:
            print(
                "  -> non-zero means the broadcast join in broadcast_engine.py isn't "
                "matching wallet_profiles correctly (check the CSV timestampFormat fix)."
            )
    except FileNotFoundError as e:
        print(f"[SKIP] {e}")


if __name__ == "__main__":
    main()