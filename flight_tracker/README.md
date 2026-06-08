# ✈ Real-Time Flight Tracking & Analytics Platform

> **Stack:** Python · DuckDB · FastAPI · Grafana · Power BI · Docker  
> **Data:** ADSB.lol Public API (no API key required)  
> **Author:** Generated for production use in VS Code

---

## Architecture

```
ADSB.lol API
     ↓
Ingestion Layer  (ingestion/ingest.py)
     ↓ raw JSON files
Bronze Layer     (bronze/load_bronze.py)   → DuckDB: flight_raw
     ↓
Silver Layer     (silver/process_silver.py)
  ├─ Cleaning    → flight_clean
  ├─ CDC         → flight_cdc
  ├─ SCD Type 2  → flight_history
  └─ Current     → flight_current
     ↓
Gold Layer       (gold/build_gold.py)
  ├─ aircraft_dimension
  ├─ airline_dimension
  ├─ flight_metrics
  ├─ flight_kpis
  ├─ region_summary
  ├─ top_operators
  └─ hourly_flight_trend
     ↓
API Layer        (api/main.py)  →  FastAPI on :8000
     ↓
Dashboards
  ├─ Grafana     :3000
  └─ Power BI    (ODBC connection)
```

---

## Project Structure

```
flight_tracker/
├── ingestion/
│   └── ingest.py              # Polls ADSB.lol API every 60s
├── bronze/
│   └── load_bronze.py         # Raw JSON → DuckDB bronze
├── silver/
│   └── process_silver.py      # Clean + CDC + SCD Type 2
├── gold/
│   └── build_gold.py          # Dimensions + KPIs + metrics
├── pipeline/
│   └── run_pipeline.py        # Full orchestrator
├── api/
│   └── main.py                # FastAPI REST service
├── dashboards/
│   ├── grafana/               # Dashboard JSON + datasource config
│   └── powerbi/               # Power Query M + DAX measures
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── tests/
│   └── test_pipeline.py
├── config/
│   ├── config.yaml            # All settings
│   └── settings.py            # Config loader
├── data/                      # Created at runtime
│   ├── raw/                   # Raw JSON files from API
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── warehouse/
│       └── flights.duckdb     # Main DuckDB database
├── logs/                      # Log files
└── requirements.txt
```

---

## SETUP — Step by Step

### Prerequisites
- Python 3.11+
- VS Code
- Docker Desktop (for Grafana)
- Git

---

### Step 1 — Clone / create project

```bash
# In VS Code terminal:
cd C:\Users\YourName\projects    # Windows
# or
cd ~/projects                    # Mac/Linux

# If using Git:
git init flight_tracker
cd flight_tracker
```

---

### Step 2 — Create virtual environment

```bash
python -m venv venv

# Activate:
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate
```

---

### Step 3 — Install dependencies

```bash
pip install -r requirements.txt
```

---

### Step 4 — Create data directories

```bash
python -c "from config.settings import ensure_dirs; ensure_dirs()"
```

Or manually:
```bash
mkdir -p data/raw data/bronze data/silver data/gold data/warehouse logs
```

---

### Step 5 — Run one pipeline cycle (test)

```bash
# Run the full pipeline once:
python pipeline/run_pipeline.py --once

# You should see:
# STEP 1/4 ── Ingestion
# STEP 2/4 ── Bronze layer
# STEP 3/4 ── Silver layer
# STEP 4/4 ── Gold layer
# Pipeline complete in X.Xs
```

---

### Step 6 — Start the FastAPI server

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

Open in browser:
- API docs: http://localhost:8000/docs
- Live flights: http://localhost:8000/flights
- KPIs: http://localhost:8000/kpis
- Map data: http://localhost:8000/map/data
- Alerts: http://localhost:8000/alerts

---

### Step 7 — Start continuous pipeline (separate terminal)

```bash
python pipeline/run_pipeline.py
```

This runs every 60 seconds automatically:
- Fetches fresh data from 4 Indian regions
- Loads into bronze
- Processes CDC + SCD2 in silver
- Rebuilds gold KPIs

---

### Step 8 — Start Grafana with Docker

```bash
cd docker
docker-compose up grafana -d
```

Open: http://localhost:3000  
Login: admin / flighttracker123

The dashboard auto-loads with:
- Active flights count
- Avg altitude & speed
- Emergency alerts
- Hourly trend charts
- Top operators

---

### Step 9 — Connect Power BI

1. Install DuckDB ODBC driver:
   https://duckdb.org/docs/api/odbc/overview

2. Create ODBC DSN named `FlightTracker`:
   - Database path: `C:\...\flight_tracker\data\warehouse\flights.duckdb`

3. Power BI Desktop → Get Data → ODBC → FlightTracker

4. Import tables:
   - `flight_current` → Live map + current stats
   - `flight_kpis` → KPI cards
   - `region_summary` → Region breakdown
   - `top_operators` → Operator charts
   - `hourly_flight_trend` → Time series

5. Copy DAX measures from `dashboards/powerbi/powerbi_setup.pq`

---

### Step 10 — Run Tests

```bash
pytest tests/ -v
```

---

## Running Everything (Full Stack)

**Terminal 1 — Pipeline (continuous ingestion):**
```bash
python pipeline/run_pipeline.py
```

**Terminal 2 — API server:**
```bash
uvicorn api.main:app --reload --port 8000
```

**Terminal 3 — Grafana:**
```bash
cd docker && docker-compose up grafana -d
```

---

## Docker (Full Deployment)

```bash
cd docker
docker-compose up --build -d
```

Services:
| Service  | URL                    |
|----------|------------------------|
| API      | http://localhost:8000  |
| Grafana  | http://localhost:3000  |

---

## API Endpoints

| Method | Endpoint              | Description                    |
|--------|-----------------------|--------------------------------|
| GET    | /flights              | Live aircraft positions        |
| GET    | /flights/all          | All cleaned records            |
| GET    | /aircraft/{icao}      | History for one aircraft       |
| GET    | /aircraft             | Aircraft dimension             |
| GET    | /airlines             | Airline stats                  |
| GET    | /metrics              | Time-series metrics            |
| GET    | /kpis                 | Current KPI snapshot           |
| GET    | /alerts               | Emergency squawk aircraft      |
| GET    | /cdc/events           | CDC event log                  |
| GET    | /regions              | Region summary                 |
| GET    | /operators            | Top 20 operators               |
| GET    | /trends/hourly        | Hourly trend data              |
| GET    | /map/data             | GeoJSON for map visualisation  |

---

## DuckDB Tables

| Layer  | Table                 | Description                        |
|--------|-----------------------|------------------------------------|
| Bronze | flight_raw            | Raw aircraft records               |
| Bronze | bronze_audit          | Load audit log                     |
| Silver | flight_clean          | Cleaned, validated records         |
| Silver | flight_cdc            | Change detection events            |
| Silver | flight_current        | Latest position per aircraft       |
| Silver | flight_history        | SCD Type 2 full history            |
| Gold   | aircraft_dimension    | Aircraft master data               |
| Gold   | airline_dimension     | Airline stats                      |
| Gold   | flight_metrics        | Time-bucketed metrics              |
| Gold   | flight_kpis           | Current KPI values                 |
| Gold   | region_summary        | Per-region aggregates              |
| Gold   | top_operators         | Top 20 operators                   |
| Gold   | hourly_flight_trend   | Hourly time series                 |

---

## Regions Monitored

| Region    | Lat   | Lon   | Radius |
|-----------|-------|-------|--------|
| Hyderabad | 17.38 | 78.48 | 250 NM |
| Delhi     | 28.61 | 77.20 | 250 NM |
| Mumbai    | 19.08 | 72.88 | 250 NM |
| Bangalore | 12.97 | 77.59 | 250 NM |

---

## Query DuckDB Directly in VS Code

Install the DuckDB VS Code extension, then open:
`data/warehouse/flights.duckdb`

Sample queries:
```sql
-- Current active flights
SELECT * FROM flight_current ORDER BY last_seen DESC LIMIT 20;

-- KPIs
SELECT * FROM flight_kpis;

-- Emergency alerts
SELECT hex, flight, squawk, emergency, lat, lon FROM flight_current WHERE is_emergency = true;

-- Top aircraft types
SELECT aircraft_type, COUNT(*) AS cnt FROM flight_current GROUP BY aircraft_type ORDER BY cnt DESC;

-- CDC events
SELECT cdc_event, COUNT(*) FROM flight_cdc GROUP BY cdc_event;

-- SCD history for an aircraft
SELECT * FROM flight_history WHERE hex = '800c9b' ORDER BY effective_from;
```

---

## Troubleshooting

**No data in DuckDB?**
- Check `logs/ingestion.log` — API may be rate-limited
- Run `python pipeline/run_pipeline.py --once` and watch terminal output

**API returns 500?**
- Run pipeline first so tables exist: `python pipeline/run_pipeline.py --once`

**Grafana shows no data?**
- Confirm `frser-sqlite-datasource` plugin is installed (auto-installed via docker-compose env var)
- Check DB path in `dashboards/grafana/datasources.yaml` matches your volume mount

**Power BI ODBC fails?**
- Use 64-bit ODBC driver and 64-bit Power BI Desktop
- Ensure the `.duckdb` file is not locked by another process
