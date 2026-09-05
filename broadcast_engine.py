"""
Member 1 — Broadcast Hash Join, v2: sourced from Neo4j instead of a Hive
table / CSV fallback.

Joins the live transaction stream against a small, in-memory-cached
per-wallet context table so downstream CEP/ML logic can factor in both:
  - historical profile (wallet age, historical risk tier) — same as
    before, just now stored as Wallet node properties in Neo4j instead
    of a Hive-managed table.
  - live graph risk (PageRank, community size, wash-ring membership) —
    new: written by graph_analytics.py's GDS algorithms, this is the
    piece that couldn't exist under the old Hive/Delta storage layer at
    all, since it depends on relationship structure a flat table can't
    represent.

Still a broadcast join, for the same reason as before: this table is
small (one row per wallet, at most low thousands of wallets for a
project at this scale), so shipping it to every executor beats a
per-record lookup.
"""
import time

import numpy as np
from pyspark.sql.functions import broadcast
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType, StructField, StructType,
)

from graph_storage import get_neo4j_driver

WALLET_CONTEXT_SCHEMA = StructType([
    StructField("wallet_address", StringType(), True),
    StructField("historical_tx_count", IntegerType(), True),
    StructField("historical_risk_tier", StringType(), True),
    StructField("live_tx_count", IntegerType(), True),
    StructField("pagerank", DoubleType(), True),
    StructField("betweenness", DoubleType(), True),
    StructField("community_id", IntegerType(), True),
    StructField("in_wash_ring", BooleanType(), True),
    StructField("graph_risk_score", DoubleType(), True),
    StructField("structural_novelty_score", DoubleType(), True),
])

# Re-querying Neo4j on every micro-batch (every ~10s per streaming_engine.py's
# trigger interval) is wasted work — graph_analytics.py only refreshes
# PageRank/community/ring flags every few minutes at most. A short TTL
# cache means new wallets/profile updates still show up quickly without
# hammering Neo4j on every trigger.
_CACHE_TTL_SECONDS = 30
_cache = {"rows": None, "fetched_at": 0.0}


def _min_max_normalize(values):
    lo, hi = min(values), max(values)
    span = (hi - lo) or 1.0
    return [(v - lo) / span for v in values]


def _fetch_wallet_context(driver):
    """Pulls every Wallet's historical + graph-analytics properties in one
    query, then derives two scalar features from them:

    - graph_risk_score: min-max-normalized PageRank and betweenness,
      blended with a flat bump for wash-ring membership. PageRank alone
      (the v1 approach) only caught money-flow hubs; betweenness adds
      layering intermediaries (see graph_analytics.py's run_betweenness)
      that PageRank can miss entirely, since a pure pass-through wallet
      that moves money on immediately doesn't accumulate incoming
      "importance" the way a hub does.
    - structural_novelty_score: each wallet's FastRP embedding distance
      from the graph-wide centroid embedding, min-max normalized. This
      is deliberately NOT hand-engineered from specific graph properties
      — it's "how structurally different is this wallet's neighborhood
      shape from the typical wallet's", learned by FastRP rather than
      assumed by us.

    Both stay single scalars (rather than exposing pagerank/betweenness/
    community/embedding separately as ML features) so
    train_isolation_forest.py's feature vector doesn't have to change
    shape every time graph_analytics.py's algorithm set changes.
    """
    with driver.session() as session:
        result = session.run(
            """
            MATCH (w:Wallet)
            RETURN w.address AS wallet_address,
                   coalesce(w.historical_tx_count, 0) AS historical_tx_count,
                   coalesce(w.historical_risk_tier, 'unknown') AS historical_risk_tier,
                   coalesce(w.live_tx_count, 0) AS live_tx_count,
                   coalesce(w.pagerank, 0.0) AS pagerank,
                   coalesce(w.betweenness, 0.0) AS betweenness,
                   w.community_id AS community_id,
                   coalesce(w.in_wash_ring, false) AS in_wash_ring,
                   w.embedding AS embedding
            """
        )
        records = [r.data() for r in result]

    if not records:
        return []

    normalized_pr = _min_max_normalize([r["pagerank"] for r in records])
    normalized_betw = _min_max_normalize([r["betweenness"] for r in records])

    # Centroid embedding across every wallet that has one. Wallets
    # created between graph_analytics.py runs (e.g. brand-new receivers
    # this batch) won't have an embedding yet — they fall back to 0.0
    # novelty rather than being dropped, since "no data yet" isn't the
    # same claim as "structurally typical".
    embeddings = [r["embedding"] for r in records if r["embedding"]]
    centroid = np.mean(np.array(embeddings), axis=0) if embeddings else None
    novelty_raw = []
    for r in records:
        if r["embedding"] and centroid is not None:
            novelty_raw.append(float(np.linalg.norm(np.array(r["embedding"]) - centroid)))
        else:
            novelty_raw.append(None)
    known = [v for v in novelty_raw if v is not None]
    normalized_novelty = _min_max_normalize(known) if known else []
    novelty_iter = iter(normalized_novelty)

    for r, pr_norm, betw_norm, novelty_val in zip(
        records, normalized_pr, normalized_betw, novelty_raw
    ):
        ring_bump = 0.3 if r["in_wash_ring"] else 0.0
        r["graph_risk_score"] = min(1.0, 0.4 * pr_norm + 0.3 * betw_norm + ring_bump)
        r["structural_novelty_score"] = next(novelty_iter) if novelty_val is not None else 0.0
        r["community_id"] = int(r["community_id"]) if r["community_id"] is not None else -1
        del r["embedding"]

    return records


def load_wallet_context(spark, driver=None, force_refresh=False):
    """Returns a small Spark DataFrame of per-wallet context, refreshing
    from Neo4j at most once per _CACHE_TTL_SECONDS."""
    now = time.time()
    if (
        force_refresh
        or _cache["rows"] is None
        or (now - _cache["fetched_at"]) > _CACHE_TTL_SECONDS
    ):
        driver = driver or get_neo4j_driver()
        _cache["rows"] = _fetch_wallet_context(driver)
        _cache["fetched_at"] = now

    rows = _cache["rows"]
    if not rows:
        return spark.createDataFrame([], WALLET_CONTEXT_SCHEMA)

    tuples = [
        (
            r["wallet_address"],
            r["historical_tx_count"],
            r["historical_risk_tier"],
            r["live_tx_count"],
            r["pagerank"],
            r["betweenness"],
            r["community_id"],
            r["in_wash_ring"],
            r["graph_risk_score"],
            r["structural_novelty_score"],
        )
        for r in rows
    ]
    return spark.createDataFrame(tuples, WALLET_CONTEXT_SCHEMA)


def join_with_profiles(stream_df, spark, driver=None):
    context = load_wallet_context(spark, driver=driver)
    return stream_df.join(
        broadcast(context),
        on="wallet_address",
        how="left",
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("SentinelDeFi-BroadcastJoinTest").getOrCreate()
    sample_stream = spark.createDataFrame(
        [("0x0000000000000000000000000000000000000005", 1200.0)],
        ["wallet_address", "amount_usd"],
    )
    joined = join_with_profiles(sample_stream, spark)
    joined.explain()
    joined.show(truncate=False)
