"""
SentinelDeFi — quick data inspector, v2.

The original version read Delta/Hive parquet files directly off disk with
pandas, specifically to avoid spinning up a SparkSession. That rationale
carries over cleanly to Neo4j: this just opens a driver connection and
runs a few read-only Cypher queries — still no JVM, no Ivy resolution,
no JAVA_HOME juggling.

Usage:
    pip install neo4j pandas
    python inspect_data.py
    python inspect_data.py --uri bolt://localhost:7687 --rows 30
"""
import argparse

import pandas as pd

from graph_storage import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER


def fetch_wallets(driver) -> pd.DataFrame:
    with driver.session() as session:
        result = session.run(
            """
            MATCH (w:Wallet)
            RETURN w.address AS wallet_address,
                   w.first_seen_ts AS first_seen_ts,
                   w.historical_tx_count AS historical_tx_count,
                   w.historical_risk_tier AS historical_risk_tier,
                   w.pagerank AS pagerank,
                   w.community_id AS community_id,
                   coalesce(w.in_wash_ring, false) AS in_wash_ring
            """
        )
        return pd.DataFrame([r.data() for r in result])


def fetch_transactions(driver, limit: int) -> pd.DataFrame:
    with driver.session() as session:
        result = session.run(
            """
            MATCH (sender:Wallet)-[r:SENT]->(receiver:Wallet)
            RETURN r.tx_id AS tx_id,
                   sender.address AS wallet_address,
                   receiver.address AS to_wallet,
                   r.amount_usd AS amount_usd,
                   r.gas_fee AS gas_fee,
                   r.anomaly_reason AS anomaly_reason,
                   r.z_score AS z_score,
                   r.ml_score AS ml_score,
                   r.ml_anomaly AS ml_anomaly,
                   r.timestamp AS timestamp
            ORDER BY r.timestamp DESC
            LIMIT $limit
            """,
            limit=limit,
        )
        return pd.DataFrame([r.data() for r in result])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default=NEO4J_URI)
    parser.add_argument("--user", default=NEO4J_USER)
    parser.add_argument("--password", default=NEO4J_PASSWORD)
    parser.add_argument(
        "--rows", type=int, default=20, help="How many transaction rows to print (default 20)"
    )
    args = parser.parse_args()

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)

    from neo4j import GraphDatabase
    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))

    print("=" * 70)
    print("WALLETS")
    print("=" * 70)
    try:
        wallets = fetch_wallets(driver)
        print(f"{len(wallets)} wallets\n")
        print(wallets.to_string(index=False))
        if "historical_risk_tier" in wallets.columns and len(wallets):
            print(f"\nrisk tier distribution:\n{wallets['historical_risk_tier'].value_counts()}")
        if "in_wash_ring" in wallets.columns and len(wallets):
            ring_count = int(wallets["in_wash_ring"].sum())
            print(f"\nwallets currently flagged in a wash-trading ring: {ring_count}")
    except Exception as e:
        print(f"[SKIP] {e}")

    print("\n" + "=" * 70)
    print("TRANSACTIONS (SENT relationships)")
    print("=" * 70)
    try:
        transactions = fetch_transactions(driver, args.rows)
        print(f"showing up to {args.rows} most recent transactions\n")
        print(transactions.to_string(index=False))
        if "anomaly_reason" in transactions.columns and len(transactions):
            print(f"\nanomaly_reason counts:\n{transactions['anomaly_reason'].value_counts(dropna=False)}")
    except Exception as e:
        print(f"[SKIP] {e}")

    driver.close()


if __name__ == "__main__":
    main()
