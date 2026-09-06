"""
Loads the synthetic historical wallet baseline profiles
(generate_wallet_profiles.py) into Neo4j as Wallet node properties.

This replaces storage_layer.py's register_wallet_profiles_table(), which
used to load the same CSV into a Hive-managed table for the Week 7
broadcast join. The join itself (broadcast_engine.py) now reads this data
back out of Neo4j instead of Hive/CSV, but it still has to get into the
graph somehow first — that's this script's job, and it's still a one-time
step (re-run any time you regenerate wallet_profiles.csv).
"""
import csv
import os

from graph_storage import DATA_DIR, get_neo4j_driver, init_graph_schema

DEFAULT_CSV_PATH = os.path.join(DATA_DIR, "wallet_profiles.csv")


def load_profiles(csv_path=DEFAULT_CSV_PATH, driver=None):
    driver = driver or get_neo4j_driver()
    init_graph_schema(driver)

    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))

    for row in rows:
        row["historical_tx_count"] = int(row["historical_tx_count"])

    cypher = """
    UNWIND $rows AS row
    MERGE (w:Wallet {address: row.wallet_address})
    SET w.first_seen_ts = row.first_seen_ts,
        w.historical_tx_count = row.historical_tx_count,
        w.historical_risk_tier = row.historical_risk_tier
    """
    with driver.session() as session:
        session.run(cypher, rows=rows)

    print(f"Loaded {len(rows)} wallet profiles from {csv_path} into Neo4j.")


if __name__ == "__main__":
    load_profiles()
