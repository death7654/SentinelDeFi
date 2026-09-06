import json
import os
from typing import Any, Dict, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from graph_storage import RUNTIME_DIR, get_neo4j_driver

app = FastAPI(title="SentinelDeFi Metrics API")

# graph_dashboard.html is opened directly from the filesystem (file://),
# which browsers treat as its own opaque origin — without this, every
# fetch() call it makes to this API gets silently blocked by CORS. Wide
# open is fine for a local class-project demo talking to localhost only;
# don't carry this setting into anything internet-facing.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# --- Persistent metrics state ------------------------------------------
# The original version kept metrics_store purely in memory, which meant
# every restart of `uvicorn metrics_api:app` silently zeroed
# total_processed/total_anomalies back to 0 with no warning — annoying
# mid-demo, and actively misleading if you're using those totals for
# anything (e.g. reporting results in a writeup). This persists the same
# dict to a small JSON file after every update and reloads it at startup.
# A full database felt like overkill for a single small dict that's
# already being updated at Kafka micro-batch cadence (a few seconds), not
# per-request — a flat file is the right amount of infrastructure here.
STATE_PATH = os.path.join(RUNTIME_DIR, "metrics_state.json")

DEFAULT_STATE: Dict[str, Any] = {
    "status": "initializing",
    "total_processed": 0,
    "total_anomalies": 0,
    "last_batch_id": None,
    "avg_z_score": 0.0,
    "recent_records": [],
}


def _load_state() -> Dict[str, Any]:
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH) as f:
                return {**DEFAULT_STATE, **json.load(f)}
        except (json.JSONDecodeError, OSError):
            pass
    return dict(DEFAULT_STATE)


def _save_state(state: Dict[str, Any]) -> None:
    """Write-to-temp-then-rename so a crash mid-write never leaves
    metrics_state.json truncated/corrupted for the next startup."""
    tmp_path = STATE_PATH + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(state, f)
        os.replace(tmp_path, STATE_PATH)
    except OSError as e:
        print(f"[WARN] Failed to persist metrics state: {e}")


metrics_store: Dict[str, Any] = _load_state()


class BatchMetricsPayload(BaseModel):
    status: str = "active"
    batch_id: int
    processed_delta: int
    anomaly_delta: int
    avg_z_score: float
    recent_records: List[Dict[str, Any]] = []


@app.post("/metrics/update")
def update_metrics(payload: BatchMetricsPayload):
    metrics_store["status"] = payload.status
    metrics_store["last_batch_id"] = payload.batch_id
    metrics_store["total_processed"] += payload.processed_delta
    metrics_store["total_anomalies"] += payload.anomaly_delta
    metrics_store["avg_z_score"] = payload.avg_z_score

    if payload.recent_records:
        metrics_store["recent_records"] = payload.recent_records[:100]

    _save_state(metrics_store)
    return {"status": "success", "batch_id": payload.batch_id}


@app.get("/metrics/summary")
def get_metrics_summary():
    recent = metrics_store.get("recent_records", [])
    record_count = len(recent)

    total_gas = (
        sum((r.get("avg_gas_fee") or 0.0) for r in recent) if record_count else 0.0
    )
    avg_gas = round(total_gas / record_count, 4) if record_count > 0 else 0.0

    total_tx = sum((r.get("tx_count") or 0) for r in recent) if record_count else 0

    return {
        "status": metrics_store["status"],
        "total_processed": metrics_store["total_processed"],
        "total_anomalies": metrics_store["total_anomalies"],
        "last_batch_id": metrics_store["last_batch_id"],
        "avg_z_score": metrics_store["avg_z_score"],
        "recent_batch_record_count": record_count,
        "recent_avg_gas_fee": avg_gas,
        "recent_total_tx_count": total_tx,
        "recent_records": recent[:20],
    }


@app.get("/health")
def health_check():
    return {"status": "healthy"}


# --- New: graph analytics endpoints ----------------------------------
# These query Neo4j directly (not the in-memory metrics_store), since
# PageRank/community/wash-ring data lives on Wallet nodes, written by
# graph_analytics.py — a separate, periodic job from the streaming
# engine's per-batch push above. Both are meant to back Grafana Infinity
# panels the same way /metrics/summary already does.

@app.get("/graph/wash-rings")
def get_wash_rings():
    """Every wallet currently flagged as sitting on a detected
    wash-trading cycle, grouped by ring id."""
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run(
            """
            MATCH (w:Wallet)
            WHERE w.in_wash_ring = true
            RETURN w.wash_ring_id AS ring_id,
                   collect(w.address) AS wallets
            ORDER BY ring_id
            """
        )
        rings = [record.data() for record in result]
    return {"ring_count": len(rings), "rings": rings}


@app.get("/graph/top-risk-wallets")
def get_top_risk_wallets(limit: int = 20):
    """Top wallets by PageRank, with their community and wash-ring
    status — the "who should a human analyst look at first" view."""
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run(
            """
            MATCH (w:Wallet)
            WHERE w.pagerank IS NOT NULL
            RETURN w.address AS wallet_address,
                   w.pagerank AS pagerank,
                   w.community_id AS community_id,
                   coalesce(w.in_wash_ring, false) AS in_wash_ring,
                   coalesce(w.historical_risk_tier, 'unknown') AS historical_risk_tier
            ORDER BY w.pagerank DESC
            LIMIT $limit
            """,
            limit=limit,
        )
        wallets = [record.data() for record in result]
    return {"wallets": wallets}


@app.get("/graph/recent-edges")
def get_recent_edges(limit: int = 150):
    """The most recent SENT relationships, for actually drawing the
    transaction graph — /graph/top-risk-wallets and /graph/wash-rings
    alone only give the dashboard *nodes*, never the real edges between
    them, which is why the live graph view previously showed a scatter
    of disconnected dots with no lines connecting them (the only "edges"
    it had were ring-membership hops, not actual transactions)."""
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run(
            """
            MATCH (sender:Wallet)-[r:SENT]->(receiver:Wallet)
            RETURN sender.address AS from_wallet,
                   receiver.address AS to_wallet,
                   r.amount_usd AS amount_usd,
                   coalesce(r.anomaly_reason, null) AS anomaly_reason,
                   r.timestamp AS timestamp
            ORDER BY r.timestamp DESC
            LIMIT $limit
            """,
            limit=limit,
        )
        edges = [record.data() for record in result]
    return {"edge_count": len(edges), "edges": edges}


@app.get("/graph/summary")
def get_graph_summary():
    """Headline graph stats for a Grafana stat panel: node/edge counts
    and how many wallets are currently ring-flagged."""
    driver = get_neo4j_driver()
    with driver.session() as session:
        counts = session.run(
            """
            MATCH (w:Wallet)
            OPTIONAL MATCH (w)-[r:SENT]->()
            RETURN count(DISTINCT w) AS wallet_count,
                   count(r) AS edge_count,
                   count(DISTINCT CASE WHEN w.in_wash_ring THEN w END) AS ring_wallet_count,
                   count(DISTINCT w.community_id) AS community_count
            """
        ).single()
    return dict(counts) if counts else {}
