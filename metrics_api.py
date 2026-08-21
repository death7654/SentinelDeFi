from fastapi import FastAPI
import duckdb
import os

app = FastAPI()

# Directory where streaming_engine.py outputs micro-batches
DELTA_PATH = "./delta_lake_sink"

@app.get("/metrics/summary")
def get_metrics():
    if not os.path.exists(DELTA_PATH):
        return {
            "status": "waiting_for_data",
            "total_processed": 0,
            "anomaly_count": 0,
            "avg_z_score": 0.0,
            "recent_records": []
        }
    
    try:
        con = duckdb.connect()
        # Enable delta extension in DuckDB
        con.execute("INSTALL delta; LOAD delta;")
        
        # Query latest records from Delta Lake table
        df = con.execute(f"SELECT * FROM delta_scan('{DELTA_PATH}') ORDER BY window_end DESC LIMIT 50").df()
        
        total_records = len(df)
        anomalies = len(df[df['Z_score'] > 3.0]) if 'Z_score' in df.columns else 0
        avg_z = float(df['Z_score'].mean()) if total_records > 0 and 'Z_score' in df.columns else 0.0
        
        return {
            "status": "active",
            "total_processed": total_records,
            "anomaly_count": anomalies,
            "avg_z_score": round(avg_z, 2),
            "recent_records": df.to_dict(orient="records")
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
