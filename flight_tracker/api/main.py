"""
STEP 5 — API LAYER
FastAPI service exposing all warehouse tables as REST endpoints.
Run: uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
"""
import sys
from pathlib import Path
from typing import Optional, List
from datetime import datetime

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import load_config, BASE_DIR

cfg     = load_config()
DB_PATH = str(BASE_DIR / cfg["paths"]["duckdb"])

app = FastAPI(
    title="Flight Tracker API",
    description="Real-Time Flight Tracking & Analytics — powered by ADSB.lol + DuckDB",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_con():
    return duckdb.connect(DB_PATH, read_only=True)


def query(sql: str, params=None):
    con = get_con()
    try:
        if params:
            result = con.execute(sql, params).df()
        else:
            result = con.execute(sql).df()
        return result.to_dict(orient="records")
    finally:
        con.close()


# ── models ────────────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    db_path: str
    timestamp: str


# ── routes ────────────────────────────────────────────────────────────────────

@app.get("/", tags=["health"])
def root():
    return {"message": "Flight Tracker API is running ✈", "docs": "/docs"}


@app.get("/health", response_model=HealthResponse, tags=["health"])
def health():
    try:
        con = get_con()
        con.execute("SELECT 1").fetchone()
        con.close()
        return {"status": "ok", "db_path": DB_PATH, "timestamp": datetime.utcnow().isoformat()}
    except Exception as e:
        raise HTTPException(503, detail=str(e))


@app.get("/flights", tags=["flights"])
def get_flights(
    region: Optional[str] = None,
    emergency_only: bool = False,
    airborne_only: bool = False,
    limit: int = Query(default=200, le=5000),
):
    """Live flight positions from flight_current."""
    filters = ["1=1"]
    params  = []
    if region:
        filters.append("region = ?")
        params.append(region)
    if emergency_only:
        filters.append("is_emergency = true")
    if airborne_only:
        filters.append("on_ground = false")

    where = " AND ".join(filters)
    sql = f"""
        SELECT hex, flight, registration, aircraft_type, squawk,
               lat, lon, altitude_ft, on_ground, gs_knots, track_deg,
               baro_rate, emergency, is_emergency, region, region_label,
               last_seen, updated_at
        FROM flight_current
        WHERE {where}
        ORDER BY last_seen DESC
        LIMIT {limit}
    """
    return query(sql, params or None)


@app.get("/flights/all", tags=["flights"])
def get_all_flights(
    region: Optional[str] = None,
    limit: int = Query(default=500, le=10000),
    offset: int = 0,
):
    """All cleaned flight records from flight_clean."""
    filters = ["1=1"]
    params  = []
    if region:
        filters.append("region = ?")
        params.append(region)
    where = " AND ".join(filters)
    sql = f"""
        SELECT * FROM flight_clean
        WHERE {where}
        ORDER BY ingested_at DESC
        LIMIT {limit} OFFSET {offset}
    """
    return query(sql, params or None)


@app.get("/aircraft/{icao}", tags=["aircraft"])
def get_aircraft(icao: str):
    """Full history for a specific aircraft by ICAO hex code."""
    rows = query(
        "SELECT * FROM flight_history WHERE hex = ? ORDER BY effective_from DESC",
        [icao.lower()]
    )
    if not rows:
        raise HTTPException(404, detail=f"Aircraft {icao} not found")
    return {"icao": icao, "history": rows, "count": len(rows)}


@app.get("/aircraft", tags=["aircraft"])
def get_all_aircraft(limit: int = Query(default=100, le=1000)):
    """Aircraft dimension table."""
    return query(f"SELECT * FROM aircraft_dimension ORDER BY last_seen DESC LIMIT {limit}")


@app.get("/airlines", tags=["airlines"])
def get_airlines():
    """Airline dimension with stats."""
    return query("SELECT * FROM airline_dimension ORDER BY flight_count DESC")


@app.get("/metrics", tags=["analytics"])
def get_metrics(
    region: Optional[str] = None,
    hours: int = Query(default=24, le=168),
):
    """Time-series flight metrics."""
    filters = [f"metric_time >= NOW() - INTERVAL '{hours}' HOUR"]
    params  = []
    if region:
        filters.append("region = ?")
        params.append(region)
    where = " AND ".join(filters)
    sql = f"SELECT * FROM flight_metrics WHERE {where} ORDER BY metric_time DESC"
    return query(sql, params or None)


@app.get("/kpis", tags=["analytics"])
def get_kpis():
    """Current KPI snapshot."""
    return query("SELECT * FROM flight_kpis ORDER BY kpi_name")


@app.get("/alerts", tags=["safety"])
def get_alerts():
    """Current emergency squawk aircraft."""
    return query("""
        SELECT hex, flight, registration, aircraft_type, squawk,
               lat, lon, altitude_ft, gs_knots, emergency,
               region, region_label, last_seen
        FROM flight_current
        WHERE is_emergency = true
        ORDER BY last_seen DESC
    """)


@app.get("/cdc/events", tags=["cdc"])
def get_cdc_events(
    event_type: Optional[str] = None,
    limit: int = Query(default=200, le=2000),
):
    """CDC event log."""
    filters = ["1=1"]
    params  = []
    if event_type:
        filters.append("cdc_event LIKE ?")
        params.append(f"%{event_type}%")
    where = " AND ".join(filters)
    sql = f"SELECT * FROM flight_cdc WHERE {where} ORDER BY detected_at DESC LIMIT {limit}"
    return query(sql, params or None)


@app.get("/regions", tags=["analytics"])
def get_regions():
    """Region summary stats."""
    return query("SELECT * FROM region_summary ORDER BY total_flights DESC")


@app.get("/operators", tags=["analytics"])
def get_top_operators():
    """Top 20 aircraft operators by flight count."""
    return query("SELECT * FROM top_operators ORDER BY rank")


@app.get("/trends/hourly", tags=["analytics"])
def get_hourly_trends(
    region: Optional[str] = None,
    hours: int = Query(default=48, le=720),
):
    """Hourly flight trends."""
    filters = [f"hour_bucket >= NOW() - INTERVAL '{hours}' HOUR"]
    params  = []
    if region:
        filters.append("region = ?")
        params.append(region)
    where = " AND ".join(filters)
    sql = f"SELECT * FROM hourly_flight_trend WHERE {where} ORDER BY hour_bucket"
    return query(sql, params or None)


@app.get("/map/data", tags=["map"])
def get_map_data():
    """GeoJSON-ready flight data for map visualisation."""
    rows = query("""
        SELECT hex, flight, registration, aircraft_type,
               lat, lon, altitude_ft, gs_knots, track_deg,
               on_ground, is_emergency, emergency, region
        FROM flight_current
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """)
    features = []
    for r in rows:
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
            "properties": {k: v for k, v in r.items() if k not in ("lat", "lon")},
        })
    return {"type": "FeatureCollection", "features": features, "count": len(features)}


if __name__ == "__main__":
    import uvicorn
    host = cfg["api_server"]["host"]
    port = cfg["api_server"]["port"]
    uvicorn.run("main:app", host=host, port=port, reload=True)
