"""
Evaluates detection accuracy against ground truth.

Every other script in this project *claims* to detect fraud. This is the
one that checks. transaction_generator.py already knows, at the moment it
emits each transaction, exactly which case it's producing (normal /
flash_loan / wash_trade / bot_burst) — that label rides along as
`true_label` all the way through streaming_engine.py into Neo4j's `SENT`
relationships. This script reads it back out and scores three detectors
against it:

  - rule-based only    (anomaly_reason set by the CEP thresholds)
  - ML only            (ml_anomaly from the Isolation Forest)
  - combined           (either fired — what actually ships in
                         anomaly_reason today)

for precision, recall, and F1, plus a confusion matrix per detector. Run
this after the pipeline has been running for a while (a few minutes of
transaction_generator.py + streaming_engine.py is plenty at this
project's volume) to get a real read on whether the CEP thresholds or the
Isolation Forest model actually need tuning.

Usage:
    python evaluate_model.py
    python evaluate_model.py --limit 5000
"""
import argparse
import json

from sklearn.metrics import classification_report, confusion_matrix

from graph_storage import get_neo4j_driver


def fetch_labeled_edges(driver, limit):
    with driver.session() as session:
        result = session.run(
            """
            MATCH ()-[r:SENT]->()
            WHERE r.true_label IS NOT NULL
            RETURN r.true_label AS true_label,
                   r.anomaly_reason AS anomaly_reason,
                   r.ml_anomaly AS ml_anomaly,
                   r.ml_score AS ml_score
            ORDER BY r.timestamp DESC
            LIMIT $limit
            """,
            limit=limit,
        )
        return [record.data() for record in result]


def score(y_true, y_pred, name):
    print(f"\n--- {name} ---")
    print(classification_report(
        y_true, y_pred, labels=[0, 1], target_names=["normal", "anomaly"],
        zero_division=0,
    ))
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    print("Confusion matrix (rows=actual, cols=predicted) [normal, anomaly]:")
    print(f"  actual normal:  {cm[0]}")
    print(f"  actual anomaly: {cm[1]}")
    return cm


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=2000,
                         help="Max number of labeled SENT edges to evaluate (default 2000)")
    parser.add_argument("--out", default="eval_report.json",
                         help="Where to write the JSON report (default eval_report.json)")
    args = parser.parse_args()

    driver = get_neo4j_driver()
    edges = fetch_labeled_edges(driver, args.limit)

    if not edges:
        print(
            "No labeled SENT edges found. Make sure streaming_engine.py has "
            "processed at least one batch since transaction_generator.py "
            "started emitting true_label (this requires the current version "
            "of both scripts — older Neo4j data written before true_label "
            "existed won't have it)."
        )
        return

    # Ground truth: anything other than "normal" is an anomaly.
    y_true = [0 if e["true_label"] == "normal" else 1 for e in edges]
    y_rule = [1 if (e["anomaly_reason"] and e["anomaly_reason"] != "ML_ISOLATION_FOREST_ANOMALY") else 0 for e in edges]
    y_ml = [1 if e["ml_anomaly"] else 0 for e in edges]
    y_combined = [1 if e["anomaly_reason"] else 0 for e in edges]

    print(f"Evaluating {len(edges)} labeled transactions "
          f"({sum(y_true)} true anomalies, {len(edges) - sum(y_true)} true normal).")

    label_counts = {}
    for e in edges:
        label_counts[e["true_label"]] = label_counts.get(e["true_label"], 0) + 1
    print(f"True label breakdown: {label_counts}")

    cm_rule = score(y_true, y_rule, "Rule-based CEP only")
    cm_ml = score(y_true, y_ml, "Isolation Forest only")
    cm_combined = score(y_true, y_combined, "Combined (current production behavior)")

    report = {
        "n_evaluated": len(edges),
        "true_label_breakdown": label_counts,
        "rule_based": classification_report(
            y_true, y_rule, labels=[0, 1], target_names=["normal", "anomaly"],
            zero_division=0, output_dict=True,
        ),
        "isolation_forest": classification_report(
            y_true, y_ml, labels=[0, 1], target_names=["normal", "anomaly"],
            zero_division=0, output_dict=True,
        ),
        "combined": classification_report(
            y_true, y_combined, labels=[0, 1], target_names=["normal", "anomaly"],
            zero_division=0, output_dict=True,
        ),
    }
    with open(args.out, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nFull report written to {args.out}")


if __name__ == "__main__":
    main()
