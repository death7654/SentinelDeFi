from fastapi import FastAPI, Request
import duckdb
import os

app = FastAPI()

# Point to the actual path configured in storage_layer.py
DELTA_PATH = r"C:\sentineldefi\delta\anomalies"

# In-memory store for running totals
latest_metrics = {
    "status": "waiting_for_data",
    "total_processed": 0,
    "anomaly_count": 0,
    "avg_z_score": 0.0,
    "recent_records": []
}

@app.post("/api/metrics")
async def update_metrics(request: Request):
    global latest_metrics
    data = await request.json()
    
    # 1. ACCUMULATE running totals using batch deltas
    latest_metrics["total_processed"] += data.get("processed_delta", 0)
    latest_metrics["anomaly_count"] += data.get("anomaly_delta", 0)
    
    # 2. UPDATE current snapshot metrics
    latest_metrics["status"] = data.get("status", "active")
    latest_metrics["avg_z_score"] = data.get("avg_z_score", 0.0)
    
    # 3. UPDATE recent in-memory records as fallback
    if "recent_records" in data and data["recent_records"]:
        new_recs = data["recent_records"]
        latest_metrics["recent_records"] = (new_recs + latest_metrics["recent_records"])[:50]
        
    return {"status": "success"}

@app.get("/metrics/summary")
def get_metrics():
    if not os.path.exists(DELTA_PATH):
        return latest_metrics
    
    try:
        con = duckdb.connect()
        con.execute("INSTALL delta; LOAD delta;")
        
        # Querying lowercase column 'timestamp' matching ANOMALY_SCHEMA
        df = con.execute(
            f"SELECT * FROM delta_scan('{DELTA_PATH}') ORDER BY timestamp DESC LIMIT 50"
        ).df()
        
        if not df.empty:
            # Convert non-serializable Pandas/DuckDB timestamp types to ISO strings
            df['timestamp'] = df['timestamp'].astype(str)
            latest_metrics["recent_records"] = df.to_dict(orient="records")
            
        return latest_metrics
    except Exception as e:
        return {"status": "error", "message": str(e), "cached_metrics": latest_metrics}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)