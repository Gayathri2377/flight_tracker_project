"""
STEP 3 — SILVER LAYER
1. Data cleaning  → flight_clean
2. CDC detection  → flight_cdc  (new / changed / unchanged)
3. SCD Type 2     → flight_history  (full historical timeline per aircraft)
Tables created: flight_clean, flight_cdc, flight_current, flight_history
"""
import sys
from pathlib import Path
from datetime import datetime, timezone

import duckdb
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import load_config, setup_logger, ensure_dirs, BASE_DIR

setup_logger("silver")
cfg     = load_config()
DB_PATH = BASE_DIR / cfg["paths"]["duckdb"]
SV      = cfg["silver"]


# ── DDL ───────────────────────────────────────────────────────────────────────

DDL_FLIGHT_CLEAN = """
CREATE TABLE IF NOT EXISTS flight_clean (
    hex             VARCHAR,
    flight          VARCHAR,
    registration    VARCHAR,
    aircraft_type   VARCHAR,
    squawk          VARCHAR,
    category        VARCHAR,
    lat             DOUBLE,
    lon             DOUBLE,
    altitude_ft     DOUBLE,   -- numeric, nulled when 'ground'
    on_ground       BOOLEAN,
    gs_knots        DOUBLE,
    track_deg       DOUBLE,
    baro_rate       DOUBLE,
    emergency       VARCHAR,
    is_emergency    BOOLEAN,
    region          VARCHAR,
    region_label    VARCHAR,
    ingested_at     TIMESTAMP,
    loaded_at       TIMESTAMP,
    -- data quality flags
    dq_missing_pos  BOOLEAN,
    dq_missing_call BOOLEAN,
    dq_bad_alt      BOOLEAN,
    dq_bad_speed    BOOLEAN
)
"""

DDL_FLIGHT_CDC = """
CREATE TABLE IF NOT EXISTS flight_cdc (
    cdc_id          VARCHAR,   -- hex + ingested_at
    hex             VARCHAR,
    flight          VARCHAR,
    cdc_event       VARCHAR,   -- NEW | POSITION_CHANGE | ALT_CHANGE | SPEED_CHANGE | HEADING_CHANGE | UNCHANGED
    old_lat         DOUBLE,
    new_lat         DOUBLE,
    old_lon         DOUBLE,
    new_lon         DOUBLE,
    old_altitude_ft DOUBLE,
    new_altitude_ft DOUBLE,
    old_gs_knots    DOUBLE,
    new_gs_knots    DOUBLE,
    old_track_deg   DOUBLE,
    new_track_deg   DOUBLE,
    detected_at     TIMESTAMP
)
"""

DDL_FLIGHT_CURRENT = """
CREATE TABLE IF NOT EXISTS flight_current (
    hex             VARCHAR PRIMARY KEY,
    flight          VARCHAR,
    registration    VARCHAR,
    aircraft_type   VARCHAR,
    squawk          VARCHAR,
    lat             DOUBLE,
    lon             DOUBLE,
    altitude_ft     DOUBLE,
    on_ground       BOOLEAN,
    gs_knots        DOUBLE,
    track_deg       DOUBLE,
    baro_rate       DOUBLE,
    emergency       VARCHAR,
    is_emergency    BOOLEAN,
    region          VARCHAR,
    region_label    VARCHAR,
    last_seen       TIMESTAMP,
    updated_at      TIMESTAMP
)
"""

DDL_FLIGHT_HISTORY = """
CREATE TABLE IF NOT EXISTS flight_history (
    history_id      VARCHAR,   -- hex + effective_from
    hex             VARCHAR,
    flight          VARCHAR,
    registration    VARCHAR,
    aircraft_type   VARCHAR,
    squawk          VARCHAR,
    lat             DOUBLE,
    lon             DOUBLE,
    altitude_ft     DOUBLE,
    on_ground       BOOLEAN,
    gs_knots        DOUBLE,
    track_deg       DOUBLE,
    baro_rate       DOUBLE,
    emergency       VARCHAR,
    is_emergency    BOOLEAN,
    region          VARCHAR,
    region_label    VARCHAR,
    -- SCD Type 2 columns
    effective_from  TIMESTAMP,
    effective_to    TIMESTAMP,
    is_current      BOOLEAN,
    change_reason   VARCHAR
)
"""


# ── helpers ───────────────────────────────────────────────────────────────────

def get_con():
    con = duckdb.connect(str(DB_PATH))
    for ddl in [DDL_FLIGHT_CLEAN, DDL_FLIGHT_CDC,
                DDL_FLIGHT_CURRENT, DDL_FLIGHT_HISTORY]:
        con.execute(ddl)
    return con


# ── CLEAN ─────────────────────────────────────────────────────────────────────

CLEAN_SQL = f"""
INSERT INTO flight_clean
SELECT
    LOWER(TRIM(hex))                          AS hex,
    NULLIF(UPPER(TRIM(flight)),'')            AS flight,
    NULLIF(UPPER(TRIM(registration)),'')      AS registration,
    NULLIF(UPPER(TRIM(aircraft_type)),'')     AS aircraft_type,
    NULLIF(TRIM(squawk),'')                   AS squawk,
    NULLIF(TRIM(category),'')                 AS category,

    CASE WHEN lat BETWEEN {SV['invalid_lat_min']} AND {SV['invalid_lat_max']}   THEN lat  END  AS lat,
    CASE WHEN lon BETWEEN {SV['invalid_lon_min']} AND {SV['invalid_lon_max']}   THEN lon  END  AS lon,

    CASE
        WHEN LOWER(alt_baro) = 'ground' THEN 0.0
        WHEN TRY_CAST(alt_baro AS DOUBLE) BETWEEN {SV['invalid_alt_min']} AND {SV['invalid_alt_max']}
            THEN TRY_CAST(alt_baro AS DOUBLE)
        ELSE NULL
    END AS altitude_ft,

    LOWER(alt_baro) = 'ground'                           AS on_ground,

    CASE WHEN gs BETWEEN {SV['invalid_speed_min']} AND {SV['invalid_speed_max']} THEN gs END AS gs_knots,

    track                                     AS track_deg,
    baro_rate,
    NULLIF(LOWER(TRIM(emergency)),'none')     AS emergency,
    emergency IS NOT NULL AND LOWER(emergency) NOT IN ('none','')  AS is_emergency,

    _region        AS region,
    _region_label  AS region_label,
    _ingested_at   AS ingested_at,
    _loaded_at     AS loaded_at,

    -- data quality flags
    (lat IS NULL OR lon IS NULL)              AS dq_missing_pos,
    flight IS NULL                             AS dq_missing_call,
    TRY_CAST(alt_baro AS DOUBLE) NOT BETWEEN {SV['invalid_alt_min']} AND {SV['invalid_alt_max']}
        AND LOWER(alt_baro) != 'ground'       AS dq_bad_alt,
    (gs IS NULL OR gs NOT BETWEEN {SV['invalid_speed_min']} AND {SV['invalid_speed_max']}) AS dq_bad_speed

FROM flight_raw
WHERE hex IS NOT NULL
  AND hex NOT IN (SELECT hex FROM flight_clean)
"""


def run_clean(con):
    before = con.execute("SELECT COUNT(*) FROM flight_clean").fetchone()[0]
    con.execute("DELETE FROM flight_clean")          # full refresh from bronze
    con.execute(CLEAN_SQL.replace("WHERE hex IS NOT NULL\n  AND hex NOT IN (SELECT hex FROM flight_clean)", "WHERE hex IS NOT NULL"))
    after  = con.execute("SELECT COUNT(*) FROM flight_clean").fetchone()[0]
    logger.info(f"flight_clean: {after} rows (was {before})")


# ── CDC ───────────────────────────────────────────────────────────────────────

def run_cdc(con):
    """Compare latest snapshot against flight_current; emit CDC events."""
    now = datetime.now(timezone.utc)

    # Latest snapshot: newest ingested_at per hex
    latest_sql = """
        SELECT DISTINCT ON (hex)
            hex, flight, lat, lon, altitude_ft, gs_knots, track_deg, ingested_at
        FROM flight_clean
        ORDER BY hex, ingested_at DESC
    """
    latest = con.execute(latest_sql).df()
    if latest.empty:
        logger.info("CDC: no data to process")
        return

    existing = con.execute("SELECT hex, lat, lon, altitude_ft, gs_knots, track_deg FROM flight_current").df()
    existing_map = existing.set_index("hex").to_dict("index") if not existing.empty else {}

    cdc_rows = []
    current_rows = []

    for _, row in latest.iterrows():
        h    = row["hex"]
        prev = existing_map.get(h)

        if prev is None:
            event = "NEW"
        else:
            changes = []
            if abs((row["lat"] or 0) - (prev["lat"] or 0)) > 0.001 or \
               abs((row["lon"] or 0) - (prev["lon"] or 0)) > 0.001:
                changes.append("POSITION_CHANGE")
            if abs((row["altitude_ft"] or 0) - (prev["altitude_ft"] or 0)) > 50:
                changes.append("ALT_CHANGE")
            if abs((row["gs_knots"] or 0) - (prev["gs_knots"] or 0)) > 5:
                changes.append("SPEED_CHANGE")
            if abs((row["track_deg"] or 0) - (prev["track_deg"] or 0)) > 2:
                changes.append("HEADING_CHANGE")
            event = "|".join(changes) if changes else "UNCHANGED"

        cdc_rows.append({
            "cdc_id":          f"{h}_{now.isoformat()}",
            "hex":             h,
            "flight":          row["flight"],
            "cdc_event":       event,
            "old_lat":         prev["lat"]         if prev else None,
            "new_lat":         row["lat"],
            "old_lon":         prev["lon"]         if prev else None,
            "new_lon":         row["lon"],
            "old_altitude_ft": prev["altitude_ft"] if prev else None,
            "new_altitude_ft": row["altitude_ft"],
            "old_gs_knots":    prev["gs_knots"]    if prev else None,
            "new_gs_knots":    row["gs_knots"],
            "old_track_deg":   prev["track_deg"]   if prev else None,
            "new_track_deg":   row["track_deg"],
            "detected_at":     now,
        })
        current_rows.append(row)

    # Insert CDC events
    import pandas as pd
    cdc_df = pd.DataFrame(cdc_rows)
    con.register("_cdc_batch", cdc_df)
    con.execute("INSERT INTO flight_cdc SELECT * FROM _cdc_batch")
    con.unregister("_cdc_batch")

    new_count     = sum(1 for r in cdc_rows if r["cdc_event"] == "NEW")
    changed_count = sum(1 for r in cdc_rows if r["cdc_event"] != "UNCHANGED" and r["cdc_event"] != "NEW")
    logger.info(f"CDC: {new_count} new, {changed_count} changed, "
                f"{len(cdc_rows)-new_count-changed_count} unchanged")


# ── SCD Type 2 ────────────────────────────────────────────────────────────────

def run_scd2(con):
    """
    SCD Type 2 — for every flight that changed or is new:
      - Close the old record (set effective_to, is_current=false)
      - Insert new record (effective_from=now, is_current=true)
    """
    now = datetime.now(timezone.utc)

    # Columns that trigger a new SCD row when changed
    tracked = ["flight", "registration", "squawk", "altitude_ft", "gs_knots", "lat", "lon"]

    snapshot_sql = """
        SELECT DISTINCT ON (hex)
            hex, flight, registration, aircraft_type, squawk,
            lat, lon, altitude_ft, on_ground, gs_knots, track_deg,
            baro_rate, emergency, is_emergency, region, region_label, ingested_at
        FROM flight_clean
        ORDER BY hex, ingested_at DESC
    """
    snap = con.execute(snapshot_sql).df()
    if snap.empty:
        return

    current_hist = con.execute(
        "SELECT hex, flight, registration, squawk, altitude_ft, gs_knots, lat, lon "
        "FROM flight_history WHERE is_current = true"
    ).df()
    hist_map = current_hist.set_index("hex").to_dict("index") if not current_hist.empty else {}

    import pandas as pd
    new_hist_rows = []
    close_hexes   = []

    for _, row in snap.iterrows():
        h    = row["hex"]
        prev = hist_map.get(h)
        reason = None

        if prev is None:
            reason = "INITIAL_LOAD"
        else:
            changes = [c for c in tracked
                       if str(row.get(c)) != str(prev.get(c))]
            if changes:
                reason = f"CHANGED:{','.join(changes)}"
                close_hexes.append(h)

        if reason:
            new_hist_rows.append({
                "history_id":    f"{h}_{now.isoformat()}",
                "hex":           h,
                "flight":        row.get("flight"),
                "registration":  row.get("registration"),
                "aircraft_type": row.get("aircraft_type"),
                "squawk":        row.get("squawk"),
                "lat":           row.get("lat"),
                "lon":           row.get("lon"),
                "altitude_ft":   row.get("altitude_ft"),
                "on_ground":     row.get("on_ground"),
                "gs_knots":      row.get("gs_knots"),
                "track_deg":     row.get("track_deg"),
                "baro_rate":     row.get("baro_rate"),
                "emergency":     row.get("emergency"),
                "is_emergency":  row.get("is_emergency"),
                "region":        row.get("region"),
                "region_label":  row.get("region_label"),
                "effective_from": now,
                "effective_to":   None,
                "is_current":     True,
                "change_reason":  reason,
            })

    # Close old rows
    if close_hexes:
        placeholders = ",".join(["?"] * len(close_hexes))
        con.execute(
            f"UPDATE flight_history SET effective_to=?, is_current=false "
            f"WHERE hex IN ({placeholders}) AND is_current=true",
            [now] + close_hexes,
        )

    if new_hist_rows:
        df = pd.DataFrame(new_hist_rows)
        con.register("_scd_batch", df)
        con.execute("INSERT INTO flight_history SELECT * FROM _scd_batch")
        con.unregister("_scd_batch")

    # Refresh flight_current
    con.execute("DELETE FROM flight_current")
    con.execute("""
        INSERT INTO flight_current
        SELECT
            hex, flight, registration, aircraft_type, squawk,
            lat, lon, altitude_ft, on_ground, gs_knots, track_deg,
            baro_rate, emergency, is_emergency, region, region_label,
            effective_from AS last_seen,
            effective_from AS updated_at
        FROM flight_history
        WHERE is_current = true
    """)

    logger.info(f"SCD2: {len(new_hist_rows)} new rows | "
                f"{len(close_hexes)} records closed | "
                f"flight_current has {con.execute('SELECT COUNT(*) FROM flight_current').fetchone()[0]} rows")


# ── entrypoint ────────────────────────────────────────────────────────────────

def run_silver(clear_first: bool = False):
    con = get_con()
    if clear_first:
        logger.warning("Clearing silver layer...")
        for t in ["flight_clean", "flight_cdc", "flight_current", "flight_history"]:
            con.execute(f"DELETE FROM {t}")

    run_clean(con)
    run_cdc(con)
    run_scd2(con)
    con.close()
    logger.info("Silver layer complete")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear", action="store_true")
    args = parser.parse_args()
    run_silver(clear_first=args.clear)
