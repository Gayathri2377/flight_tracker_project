"""
STEP 4 — GOLD LAYER
Builds analytics-ready dimension and fact tables.
Tables: aircraft_dimension, airline_dimension, airport_dimension,
        flight_metrics, flight_kpis, region_summary
"""
import sys
from pathlib import Path
from datetime import datetime, timezone

import duckdb
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import load_config, setup_logger, ensure_dirs, BASE_DIR

setup_logger("gold")
cfg     = load_config()
DB_PATH = BASE_DIR / cfg["paths"]["duckdb"]


# ── DDL ───────────────────────────────────────────────────────────────────────

DDL_AIRCRAFT_DIM = """
CREATE TABLE IF NOT EXISTS aircraft_dimension (
    hex              VARCHAR PRIMARY KEY,
    registration     VARCHAR,
    aircraft_type    VARCHAR,
    category         VARCHAR,
    first_seen       TIMESTAMP,
    last_seen        TIMESTAMP,
    total_observations BIGINT,
    is_active        BOOLEAN,
    updated_at       TIMESTAMP
)
"""

DDL_AIRLINE_DIM = """
CREATE TABLE IF NOT EXISTS airline_dimension (
    callsign_prefix  VARCHAR PRIMARY KEY,
    airline_name     VARCHAR,
    flight_count     BIGINT,
    avg_altitude_ft  DOUBLE,
    avg_speed_knots  DOUBLE,
    last_seen        TIMESTAMP,
    updated_at       TIMESTAMP
)
"""

DDL_FLIGHT_METRICS = """
CREATE TABLE IF NOT EXISTS flight_metrics (
    metric_time      TIMESTAMP,
    region           VARCHAR,
    total_flights    BIGINT,
    on_ground        BIGINT,
    airborne         BIGINT,
    avg_altitude_ft  DOUBLE,
    max_altitude_ft  DOUBLE,
    avg_speed_knots  DOUBLE,
    max_speed_knots  DOUBLE,
    emergency_count  BIGINT,
    unique_aircraft  BIGINT,
    unique_airlines  BIGINT
)
"""

DDL_FLIGHT_KPIS = """
CREATE TABLE IF NOT EXISTS flight_kpis (
    kpi_name         VARCHAR,
    kpi_value        DOUBLE,
    kpi_label        VARCHAR,
    computed_at      TIMESTAMP
)
"""

DDL_REGION_SUMMARY = """
CREATE TABLE IF NOT EXISTS region_summary (
    region           VARCHAR,
    region_label     VARCHAR,
    total_flights    BIGINT,
    active_now       BIGINT,
    avg_altitude_ft  DOUBLE,
    avg_speed_knots  DOUBLE,
    emergency_count  BIGINT,
    top_aircraft_type VARCHAR,
    computed_at      TIMESTAMP
)
"""

DDL_TOP_OPERATORS = """
CREATE TABLE IF NOT EXISTS top_operators (
    rank             INTEGER,
    operator         VARCHAR,
    flight_count     BIGINT,
    avg_altitude_ft  DOUBLE,
    avg_speed_knots  DOUBLE,
    computed_at      TIMESTAMP
)
"""

DDL_HOURLY_TREND = """
CREATE TABLE IF NOT EXISTS hourly_flight_trend (
    hour_bucket      TIMESTAMP,
    region           VARCHAR,
    flight_count     BIGINT,
    avg_altitude_ft  DOUBLE,
    avg_speed_knots  DOUBLE,
    emergency_count  BIGINT
)
"""


# ── builders ──────────────────────────────────────────────────────────────────

def build_aircraft_dim(con):
    con.execute("DELETE FROM aircraft_dimension")
    con.execute(f"""
        INSERT INTO aircraft_dimension
        SELECT
            hex,
            MAX(registration)    AS registration,
            MAX(aircraft_type)   AS aircraft_type,
            MAX(category)        AS category,
            MIN(ingested_at)     AS first_seen,
            MAX(ingested_at)     AS last_seen,
            COUNT(*)             AS total_observations,
            MAX(ingested_at) > NOW() - INTERVAL 5 MINUTE  AS is_active,
            NOW()                AS updated_at
        FROM flight_clean
        WHERE hex IS NOT NULL
        GROUP BY hex
    """)
    n = con.execute("SELECT COUNT(*) FROM aircraft_dimension").fetchone()[0]
    logger.info(f"aircraft_dimension: {n} rows")


def build_airline_dim(con):
    con.execute("DELETE FROM airline_dimension")
    con.execute(f"""
        INSERT INTO airline_dimension
        SELECT
            LEFT(flight, 3)          AS callsign_prefix,
            LEFT(flight, 3)          AS airline_name,   -- placeholder; enrich via airline DB
            COUNT(*)                 AS flight_count,
            AVG(altitude_ft)         AS avg_altitude_ft,
            AVG(gs_knots)            AS avg_speed_knots,
            MAX(ingested_at)         AS last_seen,
            NOW()                    AS updated_at
        FROM flight_clean
        WHERE flight IS NOT NULL AND LENGTH(flight) >= 3
        GROUP BY LEFT(flight, 3)
        ORDER BY flight_count DESC
    """)
    n = con.execute("SELECT COUNT(*) FROM airline_dimension").fetchone()[0]
    logger.info(f"airline_dimension: {n} rows")


def build_flight_metrics(con):
    con.execute("DELETE FROM flight_metrics")
    con.execute("""
        INSERT INTO flight_metrics
        SELECT
            DATE_TRUNC('minute', ingested_at)  AS metric_time,
            region,
            COUNT(*)                           AS total_flights,
            SUM(CASE WHEN on_ground THEN 1 ELSE 0 END) AS on_ground,
            SUM(CASE WHEN NOT on_ground THEN 1 ELSE 0 END) AS airborne,
            AVG(altitude_ft)                   AS avg_altitude_ft,
            MAX(altitude_ft)                   AS max_altitude_ft,
            AVG(gs_knots)                      AS avg_speed_knots,
            MAX(gs_knots)                      AS max_speed_knots,
            SUM(CASE WHEN is_emergency THEN 1 ELSE 0 END) AS emergency_count,
            COUNT(DISTINCT hex)                AS unique_aircraft,
            COUNT(DISTINCT LEFT(flight,3))     AS unique_airlines
        FROM flight_clean
        GROUP BY DATE_TRUNC('minute', ingested_at), region
    """)
    n = con.execute("SELECT COUNT(*) FROM flight_metrics").fetchone()[0]
    logger.info(f"flight_metrics: {n} rows")


def build_kpis(con):
    now = datetime.now(timezone.utc)
    con.execute("DELETE FROM flight_kpis")

    kpis = con.execute("""
        SELECT
            COUNT(DISTINCT hex)                                 AS active_flights,
            ROUND(AVG(altitude_ft), 0)                         AS avg_altitude,
            ROUND(AVG(gs_knots), 0)                            AS avg_speed,
            COUNT(DISTINCT CASE WHEN is_emergency THEN hex END) AS emergency_count,
            COUNT(DISTINCT aircraft_type)                       AS unique_types,
            COUNT(DISTINCT LEFT(flight,3))                      AS unique_airlines,
            SUM(CASE WHEN on_ground THEN 1 ELSE 0 END)          AS on_ground,
            SUM(CASE WHEN NOT on_ground THEN 1 ELSE 0 END)      AS airborne
        FROM flight_current
    """).fetchone()

    rows = [
        ("active_flights",   kpis[0], "Active Flights"),
        ("avg_altitude_ft",  kpis[1], "Avg Altitude (ft)"),
        ("avg_speed_knots",  kpis[2], "Avg Speed (knots)"),
        ("emergency_count",  kpis[3], "Emergency Squawks"),
        ("unique_types",     kpis[4], "Aircraft Types"),
        ("unique_airlines",  kpis[5], "Airlines"),
        ("on_ground",        kpis[6], "On Ground"),
        ("airborne",         kpis[7], "Airborne"),
    ]
    con.executemany(
        "INSERT INTO flight_kpis VALUES (?,?,?,?)",
        [[r[0], r[1], r[2], now] for r in rows]
    )
    logger.info(f"flight_kpis: {len(rows)} KPIs computed")
    for r in rows:
        logger.info(f"  {r[2]:<25} = {r[1]}")


def build_region_summary(con):
    con.execute("DELETE FROM region_summary")
    con.execute("""
        INSERT INTO region_summary
        SELECT
            fc.region,
            fc.region_label,
            COUNT(*)                    AS total_flights,
            COUNT(*)                    AS active_now,
            AVG(fc.altitude_ft)         AS avg_altitude_ft,
            AVG(fc.gs_knots)            AS avg_speed_knots,
            SUM(CASE WHEN fc.is_emergency THEN 1 ELSE 0 END) AS emergency_count,
            (
                SELECT aircraft_type FROM flight_clean fc2
                WHERE fc2.region = fc.region AND fc2.aircraft_type IS NOT NULL
                GROUP BY aircraft_type ORDER BY COUNT(*) DESC LIMIT 1
            )                           AS top_aircraft_type,
            NOW()                       AS computed_at
        FROM flight_current fc
        GROUP BY fc.region, fc.region_label
    """)
    n = con.execute("SELECT COUNT(*) FROM region_summary").fetchone()[0]
    logger.info(f"region_summary: {n} rows")


def build_top_operators(con):
    con.execute("DELETE FROM top_operators")
    con.execute("""
        INSERT INTO top_operators
        SELECT
            ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank,
            LEFT(flight, 3)     AS operator,
            COUNT(*)            AS flight_count,
            AVG(altitude_ft)    AS avg_altitude_ft,
            AVG(gs_knots)       AS avg_speed_knots,
            NOW()               AS computed_at
        FROM flight_clean
        WHERE flight IS NOT NULL AND LENGTH(flight) >= 3
        GROUP BY LEFT(flight, 3)
        ORDER BY flight_count DESC
        LIMIT 20
    """)
    n = con.execute("SELECT COUNT(*) FROM top_operators").fetchone()[0]
    logger.info(f"top_operators: {n} rows")


def build_hourly_trend(con):
    con.execute("DELETE FROM hourly_flight_trend")
    con.execute("""
        INSERT INTO hourly_flight_trend
        SELECT
            DATE_TRUNC('hour', ingested_at) AS hour_bucket,
            region,
            COUNT(DISTINCT hex)             AS flight_count,
            AVG(altitude_ft)                AS avg_altitude_ft,
            AVG(gs_knots)                   AS avg_speed_knots,
            SUM(CASE WHEN is_emergency THEN 1 ELSE 0 END) AS emergency_count
        FROM flight_clean
        GROUP BY DATE_TRUNC('hour', ingested_at), region
        ORDER BY hour_bucket
    """)
    n = con.execute("SELECT COUNT(*) FROM hourly_flight_trend").fetchone()[0]
    logger.info(f"hourly_flight_trend: {n} rows")


# ── entrypoint ────────────────────────────────────────────────────────────────

def run_gold(clear_first: bool = False):
    con = duckdb.connect(str(DB_PATH))
    for ddl in [DDL_AIRCRAFT_DIM, DDL_AIRLINE_DIM, DDL_FLIGHT_METRICS,
                DDL_FLIGHT_KPIS, DDL_REGION_SUMMARY, DDL_TOP_OPERATORS,
                DDL_HOURLY_TREND]:
        con.execute(ddl)

    if clear_first:
        logger.warning("Clearing gold layer...")
        for t in ["aircraft_dimension", "airline_dimension", "flight_metrics",
                  "flight_kpis", "region_summary", "top_operators", "hourly_flight_trend"]:
            con.execute(f"DELETE FROM {t}")

    build_aircraft_dim(con)
    build_airline_dim(con)
    build_flight_metrics(con)
    build_kpis(con)
    build_region_summary(con)
    build_top_operators(con)
    build_hourly_trend(con)

    con.close()
    logger.info("Gold layer complete")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear", action="store_true")
    args = parser.parse_args()
    run_gold(clear_first=args.clear)
