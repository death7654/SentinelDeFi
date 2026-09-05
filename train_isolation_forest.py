"""
Trains the anomaly-scoring model consumed by streaming_engine.py, v2:
Isolation Forest instead of KMeans.

Why this is an upgrade, not just a swap:
  - KMeans forced every window into exactly one of k hard clusters, and
    train_kmeans.py's identify_anomaly_clusters() then had to *guess*
    which clusters were "anomalous" by assuming the majority cluster is
    normal. That assumption breaks the moment anomaly traffic isn't rare
    (e.g. during a coordinated attack) and gives no sense of *how*
    anomalous a point is — a window barely on the wrong side of a
    centroid boundary looks identical to an extreme outlier.
  - Isolation Forest scores each point by how few random partitions it
    takes to isolate it — outliers get isolated fast, so they get a
    clearly separated continuous score, not a coin-flip cluster
    assignment. It doesn't assume the data forms convex clusters at all
    (KMeans does, via Euclidean distance-to-centroid), which matters here
    because bot-burst and flash-loan anomalies are two geometrically
    different shapes in feature space, not one.
  - It also degrades gracefully with more features. This version adds
    two: graph_risk_score (PageRank + betweenness + wash-ring flag) and
    structural_novelty_score (FastRP embedding distance from the
    graph-wide centroid), both from graph_analytics.py via
    broadcast_engine.py — which KMeans's centroid-distance approach would
    have handled poorly once features have very different scales and
    distributions.

Features: [z_score, tx_count, avg_gas_fee, graph_risk_score,
structural_novelty_score] — the first three are exactly what
streaming_engine.py's cep_stream already computes per (window,
wallet_address, to_wallet); the last two are added by broadcast_engine.py
from Neo4j (0.0 if the wallet has no graph analytics run against it yet,
e.g. a brand-new wallet).

Synthetic training data: same rationale as train_kmeans.py — no
historical batch history exists yet, so this generates synthetic
"normal" and "anomalous" windows whose feature ranges match what the
live pipeline actually produces. Once sentineldefi has real historical
data in Neo4j, swap generate_synthetic_training_data() for a query
pulling real (z_score, tx_count, avg_gas_fee, graph_risk_score) tuples
off the :SENT relationships instead.
"""
import json

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest

from graph_storage import ISOFOREST_MODEL_PATH, ISOFOREST_META_PATH

FEATURE_COLS = ["z_score", "tx_count", "avg_gas_fee", "graph_risk_score", "structural_novelty_score"]
SEED = 42

# Expected fraction of anomalous traffic — transaction_generator.py emits
# ~2% (1% flash-loan + 0.5% wash-trading + 0.5% bot-burst). Isolation
# Forest uses this directly as its `contamination` parameter to calibrate
# the decision threshold, rather than KMeans's indirect "smallest
# cluster(s)" proxy for the same idea.
CONTAMINATION = 0.02


def generate_synthetic_training_data(seed=SEED, n_normal=1200, n_anomaly=40):
    """Synthesizes (z_score, tx_count, avg_gas_fee, graph_risk_score) rows
    in two rough regimes, at roughly the pipeline's real ~98/2 normal-to-
    anomalous ratio (unlike train_kmeans.py's ~200/60 split, which was
    nowhere close to the ~2% contamination the live traffic mix actually
    produces)."""
    rng = np.random.default_rng(seed)

    # Normal windows: low outlier statistic, few tx per window, cheap
    # gas, low graph risk (most legit wallets aren't central hubs and
    # aren't sitting on a detected wash-trading ring), low structural
    # novelty (most legit wallets look structurally like most other
    # legit wallets).
    normal = np.column_stack([
        rng.uniform(0.0, 2.0, n_normal),          # z_score
        rng.integers(1, 6, n_normal),              # tx_count
        rng.uniform(0.001, 0.05, n_normal),        # avg_gas_fee
        rng.beta(1.5, 8.0, n_normal),               # graph_risk_score, skewed low
        rng.beta(1.5, 8.0, n_normal),               # structural_novelty_score, skewed low
    ])

    # Anomalous windows: bot-burst-like (high count, moderate gas,
    # moderate graph risk, moderate novelty) and flash-loan/wash-trade/
    # layering-like (high z_score or high graph risk from ring
    # membership or betweenness, expensive gas, high structural novelty
    # — a wash-trade ring or a layering chain looks nothing like a
    # typical wallet's neighborhood).
    half = n_anomaly // 2
    bot_like = np.column_stack([
        rng.uniform(2.0, 6.0, half),
        rng.integers(9, 16, half),
        rng.uniform(0.05, 0.2, half),
        rng.beta(2.0, 3.0, half),
        rng.beta(2.0, 3.0, half),
    ])
    flash_like = np.column_stack([
        rng.uniform(4.0, 12.0, n_anomaly - half),
        rng.integers(1, 4, n_anomaly - half),
        rng.uniform(1.5, 5.0, n_anomaly - half),
        rng.beta(4.0, 2.0, n_anomaly - half),  # ring/layering -> high graph risk
        rng.beta(4.0, 2.0, n_anomaly - half),  # -> high structural novelty
    ])
    anomaly = np.vstack([bot_like, flash_like])

    X = np.vstack([normal, anomaly])
    return X


def main():
    X = generate_synthetic_training_data()

    model = IsolationForest(
        n_estimators=200,
        contamination=CONTAMINATION,
        random_state=SEED,
    )
    model.fit(X)

    scores = model.decision_function(X)
    predictions = model.predict(X)  # -1 = anomaly, 1 = normal

    joblib.dump(model, ISOFOREST_MODEL_PATH)
    with open(ISOFOREST_META_PATH, "w") as f:
        json.dump(
            {
                "feature_cols": FEATURE_COLS,
                "contamination": CONTAMINATION,
                # decision_function() has no fixed 0-anomaly boundary the
                # way KMeans cluster IDs did — expose the exact threshold
                # sklearn used so streaming_engine.py doesn't have to
                # re-derive it (it's simply "score < 0" per sklearn's own
                # convention, but pinning it here keeps the two files
                # from silently drifting if that convention ever changes).
                "anomaly_score_threshold": 0.0,
            },
            f,
        )

    n_flagged = int((predictions == -1).sum())
    print(f"SUCCESS: Isolation Forest saved to: {ISOFOREST_MODEL_PATH}")
    print(f"Metadata written to: {ISOFOREST_META_PATH}")
    print(f"Trained on {len(X)} synthetic windows, contamination={CONTAMINATION}")
    print(f"Flagged {n_flagged} of {len(X)} training windows as anomalous "
          f"({n_flagged / len(X):.1%})")
    print(f"Score range: min={scores.min():.3f}, max={scores.max():.3f}, "
          f"mean={scores.mean():.3f} (more negative = more anomalous)")


if __name__ == "__main__":
    main()
