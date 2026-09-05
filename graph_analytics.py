"""
Graph-native fraud detection — the actual "next level" piece Delta Lake
had no way to express.

Flat-table anomaly detection (the original KMeans-on-three-columns setup)
can only ever look at one wallet's own window statistics in isolation. It
has no way to answer relationship questions like "do these four wallets
keep passing the same money around in a circle?" — that's a structural
property of the transaction *graph*, not a statistic of any single row.

This script runs five graph algorithms over the Neo4j transaction graph
via the Graph Data Science (GDS) library, and writes the results back
onto Wallet nodes so streaming_engine.py's broadcast join
(broadcast_engine.py) can pull them in as ML features on the next batch:

  1. PageRank         -> w.pagerank          (money-flow "importance")
  2. Betweenness       -> w.betweenness       (how often a wallet sits
                                                *between* two other
                                                wallets on the shortest
                                                path connecting them —
                                                catches layering, i.e.
                                                A -> B -> C -> D where D
                                                cashes out with no cycle
                                                at all, which cycle
                                                detection below cannot
                                                see since there's no loop)
  3. Louvain           -> w.community_id      (clusters of wallets that
                                                transact heavily with each
                                                other — legitimate trading
                                                communities vs. isolated
                                                suspicious cliques)
  4. FastRP embeddings -> w.embedding         (learned structural fingerprint
                                                per wallet, used downstream by
                                                broadcast_engine.py to compute
                                                a "how structurally unusual is
                                                this wallet compared to
                                                everyone else" score without
                                                us having to hand-engineer
                                                what "unusual" means)
  5. Cycle detection   -> w.in_wash_ring,
                          w.wash_ring_id      (wallets sitting on a short
                                                directed cycle — the
                                                structural signature of
                                                wash trading)

Run this periodically (every N minutes is plenty for a project this
size) rather than per-micro-batch — GDS graph projection + algorithm runs
are not designed to run at streaming micro-batch latency.
"""
import argparse

from graph_storage import get_neo4j_driver

# Kept small on purpose: this project's synthetic graph tops out at a few
# thousand wallets, so an 8-dimensional FastRP embedding is already more
# than enough to separate "typical" wallets from structurally odd ones —
# a much higher dimension would just be noise to fit against with this
# little data.
EMBEDDING_DIMENSION = 8

GRAPH_NAME = "sentineldefi-tx-graph"

# Wash-trading rings in this project's synthetic data are 4 wallets, but
# real rings vary in size — 3..8 hops covers everything from a direct
# A->B->A round-trip up to a loosely-laundered longer loop, without the
# combinatorial blowup of searching arbitrarily long cycles on every run.
MIN_RING_LENGTH = 3
MAX_RING_LENGTH = 8
CYCLE_QUERY_TIMEOUT_SECONDS = 30


def project_graph(session):
    """(Re)creates the in-memory GDS graph projection. Dropping first
    makes this safe to re-run — GDS projections are named, in-memory, and
    error if you try to project over an existing name."""
    session.run(f"CALL gds.graph.drop('{GRAPH_NAME}', false)")
    session.run(
        f"""
        CALL gds.graph.project(
            '{GRAPH_NAME}',
            'Wallet',
            {{
                SENT: {{
                    orientation: 'NATURAL',
                    properties: 'amount_usd'
                }}
            }}
        )
        """
    )


def run_pagerank(session):
    """Money-flow importance: a wallet that receives from many other
    important wallets (weighted by amount_usd) ranks higher. Feeds
    train_isolation_forest.py as a graph_risk feature — an
    otherwise-unremarkable transaction from a wallet that PageRank has
    flagged as a major hub is a different risk profile than the same
    transaction from an obscure, brand-new wallet."""
    session.run(
        f"""
        CALL gds.pageRank.write('{GRAPH_NAME}', {{
            relationshipWeightProperty: 'amount_usd',
            writeProperty: 'pagerank'
        }})
        """
    )


def run_betweenness(session):
    """Betweenness centrality: counts how often a wallet lies on the
    shortest path between two other wallets. This is the structural
    signal that catches *layering* — funds routed A -> B -> C -> D to
    obscure their origin — which never forms a cycle and so is
    completely invisible to detect_wash_rings() below. A wallet that
    keeps showing up as a middle hop between otherwise-unrelated wallets
    is exactly what a layering intermediary looks like."""
    session.run(
        f"""
        CALL gds.betweenness.write('{GRAPH_NAME}', {{
            writeProperty: 'betweenness'
        }})
        """
    )


def run_fastrp(session):
    """FastRP node embeddings: a learned, fixed-length vector per wallet
    that summarizes its position in the graph (who it transacts with,
    how heavily, and who those wallets transact with in turn) — without
    us having to decide in advance which structural properties matter.
    broadcast_engine.py turns this into a single structural_novelty_score
    feature by measuring each wallet's distance from the graph's typical
    (centroid) embedding, so the Isolation Forest can catch structurally
    odd wallets that don't necessarily have high PageRank, high
    betweenness, or ring membership — just an unusual *shape* of
    connections."""
    session.run(
        f"""
        CALL gds.fastRP.write('{GRAPH_NAME}', {{
            embeddingDimension: {EMBEDDING_DIMENSION},
            relationshipWeightProperty: 'amount_usd',
            writeProperty: 'embedding'
        }})
        """
    )


def run_louvain(session):
    """Community detection: groups wallets that transact heavily with
    each other into the same community_id. A wallet whose community is
    tiny and disconnected from the rest of the graph (i.e. a clique that
    mostly only trades with itself) is a much stronger anomaly signal
    than transaction size alone."""
    session.run(
        f"""
        CALL gds.louvain.write('{GRAPH_NAME}', {{
            relationshipWeightProperty: 'amount_usd',
            writeProperty: 'community_id'
        }})
        """
    )

def detect_wash_rings(session):
    """... (see comment in the function for the full explanation) ..."""
    session.run(
        "MATCH (w:Wallet) SET w.in_wash_ring = false, w.wash_ring_id = null"
    )

    session.run("MATCH ()-[r:TRANSACTED_WITH]->() DELETE r")
    session.run(
        "MATCH (a:Wallet)-[:SENT]->(b:Wallet) MERGE (a)-[:TRANSACTED_WITH]->(b)"
    )

    result = session.run(
        Query(
            f"""
            MATCH p = (start:Wallet)-[:TRANSACTED_WITH*{MIN_RING_LENGTH}..{MAX_RING_LENGTH}]->(start)
            RETURN DISTINCT start.address AS start_address,
                   [n IN nodes(p) | n.address] AS ring_wallets,
                   length(p) AS ring_length
            LIMIT 100
            """,
            timeout=CYCLE_QUERY_TIMEOUT_SECONDS,
        )
    )
    rings = [record.data() for record in result]

    for ring_id, ring in enumerate(rings):
        session.run(
            """
            UNWIND $addresses AS addr
            MATCH (w:Wallet {address: addr})
            SET w.in_wash_ring = true,
                w.wash_ring_id = coalesce(w.wash_ring_id, $ring_id)
            """,
            addresses=ring["ring_wallets"],
            ring_id=ring_id,
        )

def main(drop_only=False):
    driver = get_neo4j_driver()
    with driver.session() as session:
        print(f"Projecting graph '{GRAPH_NAME}' from Wallet/SENT...")
        project_graph(session)
        if drop_only:
            return

        print("Running PageRank...")
        run_pagerank(session)

        print("Running betweenness centrality (layering detection)...")
        run_betweenness(session)

        print("Running Louvain community detection...")
        run_louvain(session)

        print(f"Running FastRP node embeddings (dim={EMBEDDING_DIMENSION})...")
        run_fastrp(session)

        print(f"Detecting wash-trading rings ({MIN_RING_LENGTH}-{MAX_RING_LENGTH} hops)...")
        rings = detect_wash_rings(session)

        session.run(f"CALL gds.graph.drop('{GRAPH_NAME}', false)")

    print(f"\nDone. Found {len(rings)} candidate wash-trading ring(s).")
    for ring in rings[:10]:
        print(
            f"  ring {rings.index(ring)}: {ring['ring_length']} hops, "
            f"wallets: {', '.join(a[:10] + '...' for a in ring['ring_wallets'][:-1])}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--drop-only", action="store_true",
        help="Just project the graph then drop it (sanity check that GDS is installed)."
    )
    args = parser.parse_args()
    main(drop_only=args.drop_only)
