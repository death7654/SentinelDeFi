"""
Trains the KMeans model consumed by streaming_engine.py as a second,
learned anomaly signal alongside the rule-based CEP thresholds.

Features: [z_score, tx_count, avg_gas_fee] — exactly the three columns
streaming_engine.py's windowed_stats/cep_stream produces per (window,
wallet_address), so the fitted PipelineModel can be applied directly to
the live stream with no renaming/reshaping at inference time.

Synthetic training data: no historical batch history exists yet (this is
the model's first training run), so this generates synthetic "normal" and
"anomalous" windows whose feature ranges match what the actual pipeline
produces:
  - z_score: the Grubbs-style (max_amount - mean_amount) / stddev_amount
    statistic computed in streaming_engine.py, not a plain z-score.
  - tx_count: transactions per wallet per 1-minute sliding window.
  - avg_gas_fee: matches transaction_generator.py's actual emitted ranges
    (normal ~0.001-0.05, bot-burst ~0.05-0.2, flash-loan ~1.5-5.0) — the
    original dummy data used gas_fee values of 10-200, three orders of
    magnitude off from what the live stream actually emits, which would
    have made every real transaction look identical to KMeans regardless
    of cluster.
Once the Delta anomalies table has enough real batches, swap this out for
real historical (z_score, tx_count, avg_gas_fee) rows read from
sentineldefi.anomalies.
"""
import json
import os
import sys

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession

from storage_layer import KMEANS_MODEL_META_PATH, KMEANS_MODEL_PATH

# 1. Force PySpark workers to use the exact active Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 2. Windows Native Hadoop Configuration (prevents winutils warning)
os.makedirs(r"C:\hadoop\logs", exist_ok=True)
os.environ["HADOOP_HOME"] = r"C:\hadoop"
if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
    os.environ["PATH"] += r";C:\hadoop\bin"

FEATURE_COLS = ["z_score", "tx_count", "avg_gas_fee"]
SEED = 42


def generate_synthetic_training_data(seed=SEED, n_normal=200, n_anomaly=60):
    """Synthesizes (z_score, tx_count, avg_gas_fee) rows in two rough
    regimes, matching transaction_generator.py's actual emitted ranges,
    so KMeans has enough points to fit a stable pair of centroids instead
    of 6 hand-picked ones."""
    rng = np.random.default_rng(seed)

    # Normal windows: low outlier statistic, few tx per window, cheap gas.
    normal = np.column_stack([
        rng.uniform(0.0, 2.0, n_normal),        # z_score
        rng.integers(1, 6, n_normal),            # tx_count
        rng.uniform(0.001, 0.05, n_normal),      # avg_gas_fee
    ])

    # Anomalous windows: mix of bot-burst-like (high count, moderate gas)
    # and flash-loan-like (high z_score, expensive gas) regimes.
    half = n_anomaly // 2
    bot_like = np.column_stack([
        rng.uniform(2.0, 6.0, half),
        rng.integers(9, 16, half),
        rng.uniform(0.05, 0.2, half),
    ])
    flash_like = np.column_stack([
        rng.uniform(4.0, 12.0, n_anomaly - half),
        rng.integers(1, 4, n_anomaly - half),
        rng.uniform(1.5, 5.0, n_anomaly - half),
    ])
    anomaly = np.vstack([bot_like, flash_like])

    rows = np.vstack([normal, anomaly])
    return [(float(z), int(c), float(g)) for z, c, g in rows]


def identify_anomaly_clusters(model, training_df):
    """KMeans cluster IDs (0, 1, 2, ...) are arbitrary and not guaranteed
    to mean the same thing across retrainings. Rather than guessing from
    centroid geometry (fragile when anomalies span more than one distinct
    behavior — e.g. bot-burst is high-tx_count/low-value while flash-loan
    is low-tx_count/high-value, so no single centroid dimension cleanly
    separates "anomalous" from "normal"), this scores the model on its
    own training data and treats whichever cluster captured the most
    points as "normal" — anomalies are rare by construction in this
    pipeline (~2% of generated traffic; see transaction_generator.py), so
    the majority cluster is normal traffic and every other cluster is
    anomalous, regardless of which specific anomaly pattern it represents
    or how many clusters k is set to.
    """
    counts = (
        model.transform(training_df)
        .groupBy("prediction")
        .count()
        .collect()
    )
    counts_by_cluster = {int(r["prediction"]): int(r["count"]) for r in counts}
    normal_cluster = max(counts_by_cluster, key=counts_by_cluster.get)
    anomaly_clusters = [c for c in counts_by_cluster if c != normal_cluster]
    return normal_cluster, anomaly_clusters, counts_by_cluster


def main():
    spark = (
        SparkSession.builder.appName("TrainKMeansModel")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dderby.stream.error.file=C:/hadoop/logs/derby.log",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    data = generate_synthetic_training_data()
    df = spark.createDataFrame(data, FEATURE_COLS)

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    # Features are on very different scales (z_score ~0-12, tx_count
    # ~1-15, avg_gas_fee ~0.001-5.0); without scaling, KMeans' Euclidean
    # distance would be dominated by whichever feature happens to have
    # the largest raw magnitude rather than reflecting all three signals.
    scaler = StandardScaler(
        inputCol="features", outputCol="scaledFeatures", withMean=True, withStd=True
    )
    # k=3, not 2: the synthetic anomalies deliberately cover two distinct
    # behaviors (bot-burst: high tx_count/low value; flash-loan: low
    # tx_count/high value). A single "anomaly" centroid averaged across
    # both would sit somewhere between them and match neither well. Three
    # clusters lets KMeans separate normal / bot-burst-like / flash-loan-
    # like; identify_anomaly_clusters() below treats any non-majority
    # cluster as anomalous, so this generalizes to any k.
    kmeans = KMeans(
        k=3, seed=SEED, featuresCol="scaledFeatures", predictionCol="prediction"
    )
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])

    model = pipeline.fit(df)
    normal_cluster, anomaly_clusters, counts = identify_anomaly_clusters(model, df)

    model.write().overwrite().save(KMEANS_MODEL_PATH)
    with open(KMEANS_MODEL_META_PATH, "w") as f:
        json.dump(
            {"normal_cluster": normal_cluster, "anomaly_clusters": anomaly_clusters},
            f,
        )

    print(f"\nSUCCESS: KMeans model saved to: {KMEANS_MODEL_PATH}")
    print(f"Cluster membership counts: {counts}")
    print(f"Normal cluster: {normal_cluster}  |  Anomaly clusters: {anomaly_clusters}")
    print(f"Metadata written to: {KMEANS_MODEL_META_PATH}")
    for i, center in enumerate(model.stages[-1].clusterCenters()):
        tag = "normal" if i == normal_cluster else "ANOMALY"
        print(f"  cluster {i} [{tag}] scaled centroid: {center}")

    spark.stop()


if __name__ == "__main__":
    main()
