"""
Generates synthetic historical wallet baseline profiles used by the
Week 7 Broadcast Hash Join. In production this table would come from
real historical on-chain analytics; here we fabricate a small lookup
table with the same shape so the join logic can be built and tested
now, ahead of Week 7.

Run this once, then run storage_layer.py to register the output as
a Hive table.
"""
import csv
import os
import random
from datetime import datetime, timedelta, timezone

# Resolve the absolute path of the current directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(BASE_DIR, "wallet_profiles.csv")

WALLETS = [f"0x{i:040x}" for i in range(1, 11)]
RISK_TIERS = ["low", "medium", "high"]


def generate_profiles():
    rows = []
    now = datetime.now(timezone.utc)
    for wallet in WALLETS:
        first_seen = now - timedelta(days=random.randint(30, 900))
        rows.append({
            "wallet_address": wallet,
            "first_seen_ts": first_seen.isoformat(sep=" ", timespec="seconds"),
            "historical_tx_count": random.randint(5, 5000),
            "historical_risk_tier": random.choices(
                RISK_TIERS, weights=[0.7, 0.25, 0.05]
            )[0],
        })
    return rows


def write_csv(rows, path=OUTPUT_PATH):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} wallet profiles to {path}")


if __name__ == "__main__":
    write_csv(generate_profiles())