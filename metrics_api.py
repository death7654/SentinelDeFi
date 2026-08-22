from typing import Any, Dict, List, Optional
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="SentinelDeFi Metrics API")

# Global In-Memory State
metrics_store: Dict[str, Any] = {
    "status": "initializing",
    "total_processed": 0,
    "total_anomalies": 0,
    "last_batch_id": None,
    "avg_z_score": 0.0,
    "recent_records": [],
}


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

  # Keep the most recent 100 records for API endpoints
  if payload.recent_records:
    metrics_store["recent_records"] = payload.recent_records[:100]

  return {"status": "success", "batch_id": payload.batch_id}


@app.get("/metrics/summary")
def get_metrics_summary():
  recent = metrics_store.get("recent_records", [])
  record_count = len(recent)

  # Safe aggregation preventing NoneType/TypeError exceptions
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
      "recent_records": recent[:20],  # Return latest 20 for UI/Grafana
  }


@app.get("/health")
def health_check():
  return {"status": "healthy"}