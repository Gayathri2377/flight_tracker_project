"""
STEP 2 — BRONZE LAYER
Reads raw JSON files from data/raw/ and loads them into DuckDB.
Append-only. Preserves full original payload + audit columns.
Tables: flight_raw
"""
import json
import sys
import glob
from pathlib import Path
from datetime import datetime, timezone

import duckdb
import pandas as pd
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import load_config, setup_logger, ensure_dirs, BASE_DIR

setup_logger("bronze")
cfg  = load_config()
RAW_DIR  = BASE_DIR / cfg["paths"]["raw_data"]
DB_PATH  = BASE_DIR / cfg["paths"]["duckdb"]


# ── schema ────────────────────────────────────────────────────────────────────

DDL_FLIGHT_RAW = """
CREATE TABLE IF NOT EXISTS flight_raw (
    -- Primary identifiers
    hex               VARCHAR,
    flight            VARCHAR,
    registration      VARCHAR,
    aircraft_type     VARCHAR,
    squawk            VARCHAR,
    category          VARCHAR,

    -- Position
    lat               DOUBLE,
    lon               DOUBLE,
    alt_baro          VARCHAR,   -- can be 'ground' so keep as text
    alt_geom          DOUBLE,

    -- Movement
    gs                DOUBLE,    -- ground speed knots
    ias               DOUBLE,
    tas               DOUBLE,
    mach              DOUBLE,
    track             DOUBLE,
    track_rate        DOUBLE,
    roll              DOUBLE,
    baro_rate         DOUBLE,
    geom_rate         DOUBLE,

    -- Navigation
    nav_qnh           DOUBLE,
    nav_altitude_mcp  DOUBLE,
    nav_altitude_fms  DOUBLE,
    nav_heading       DOUBLE,

    -- Signal quality
    seen              DOUBLE,
    seen_pos          DOUBLE,
    rssi              DOUBLE,
    messages          BIGINT,

    -- Emergency / alerts
    emergency         VARCHAR,
    alert             INTEGER,
    spi               INTEGER,

    -- Metadata
    _region           VARCHAR,
    _region_label     VARCHAR,
    _ingested_at      TIMESTAMP,
    _source_url       VARCHAR,
    _loaded_at        TIMESTAMP,
    _source_file      VARCHAR,

    -- Full raw JSON for forensics
    _raw_json         VARCHAR
)
"""

DDL_BRONZE_AUDIT = """
CREATE TABLE IF NOT EXISTS bronze_audit (
    file_name       VARCHAR,
    region          VARCHAR,
    ingested_at     TIMESTAMP,
    records_loaded  INTEGER,
    loaded_at       TIMESTAMP,
    status          VARCHAR,
    error_msg       VARCHAR
)
"""


def get_connection():
    ensure_dirs()
    con = duckdb.connect(str(DB_PATH))
    con.execute(DDL_FLIGHT_RAW)
    con.execute(DDL_BRONZE_AUDIT)
    return con


def _safe(d: dict, key, default=None):
    v = d.get(key, default)
    return v if v != "" else default


def aircraft_to_row(ac: dict, source_file: str) -> dict:
    """Flatten one aircraft dict into a bronze row."""
    return {
        "hex":              _safe(ac, "hex"),
        "flight":           _safe(ac, "flight", "").strip() or None,
        "registration":     _safe(ac, "r"),
        "aircraft_type":    _safe(ac, "t"),
        "squawk":           _safe(ac, "squawk"),
        "category":         _safe(ac, "category"),
        "lat":              _safe(ac, "lat"),
        "lon":              _safe(ac, "lon"),
        "alt_baro":         str(_safe(ac, "alt_baro", "")) or None,
        "alt_geom":         _safe(ac, "alt_geom"),
        "gs":               _safe(ac, "gs"),
        "ias":              _safe(ac, "ias"),
        "tas":              _safe(ac, "tas"),
        "mach":             _safe(ac, "mach"),
        "track":            _safe(ac, "track"),
        "track_rate":       _safe(ac, "track_rate"),
        "roll":             _safe(ac, "roll"),
        "baro_rate":        _safe(ac, "baro_rate"),
        "geom_rate":        _safe(ac, "geom_rate"),
        "nav_qnh":          _safe(ac, "nav_qnh"),
        "nav_altitude_mcp": _safe(ac, "nav_altitude_mcp"),
        "nav_altitude_fms": _safe(ac, "nav_altitude_fms"),
        "nav_heading":      _safe(ac, "nav_heading"),
        "seen":             _safe(ac, "seen"),
        "seen_pos":         _safe(ac, "seen_pos"),
        "rssi":             _safe(ac, "rssi"),
        "messages":         _safe(ac, "messages"),
        "emergency":        _safe(ac, "emergency"),
        "alert":            _safe(ac, "alert"),
        "spi":              _safe(ac, "spi"),
        "_region":          _safe(ac, "_region"),
        "_region_label":    _safe(ac, "_region_label"),
        "_ingested_at":     _safe(ac, "_ingested_at"),
        "_source_url":      _safe(ac, "_source_url"),
        "_loaded_at":       datetime.now(timezone.utc).isoformat(),
        "_source_file":     source_file,
        "_raw_json":        json.dumps(ac),
    }


def load_file(con: duckdb.DuckDBPyConnection, filepath: Path) -> int:
    """Load one raw JSON file into bronze. Returns rows inserted."""
    with open(filepath, "r") as fh:
        payload = json.load(fh)

    aircraft = payload.get("aircraft", [])
    if not aircraft:
        logger.warning(f"No aircraft in {filepath.name}")
        return 0

    rows = [aircraft_to_row(ac, filepath.name) for ac in aircraft]
    df   = pd.DataFrame(rows)

    # Convert timestamp columns
    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], utc=True, errors="coerce")
    df["_loaded_at"]   = pd.to_datetime(df["_loaded_at"],   utc=True, errors="coerce")

    con.register("_bronze_batch", df)
    con.execute("INSERT INTO flight_raw SELECT * FROM _bronze_batch")
    con.unregister("_bronze_batch")
    return len(rows)


def load_all_raw(clear_first: bool = False):
    """Load all unprocessed raw JSON files into bronze."""
    con = get_connection()

    if clear_first:
        logger.warning("Clearing bronze layer...")
        con.execute("DELETE FROM flight_raw")
        con.execute("DELETE FROM bronze_audit")

    # Find all raw files
    pattern = str(RAW_DIR / "**" / "*.json")
    files   = sorted(glob.glob(pattern, recursive=True))
    logger.info(f"Found {len(files)} raw files to process")

    # Track already-loaded files to avoid duplicates
    loaded = set(
        r[0] for r in con.execute("SELECT DISTINCT _source_file FROM flight_raw").fetchall()
    )

    total_rows = 0
    for fpath in files:
        fp = Path(fpath)
        if fp.name in loaded:
            continue
        try:
            n = load_file(con, fp)
            total_rows += n
            con.execute(
                "INSERT INTO bronze_audit VALUES (?,?,?,?,?,?,?)",
                [fp.name, fp.parent.name, datetime.now(timezone.utc),
                 n, datetime.now(timezone.utc), "OK", None]
            )
            logger.info(f"Loaded {n:>5} rows ← {fp.name}")
        except Exception as exc:
            logger.error(f"Failed {fp.name}: {exc}")
            con.execute(
                "INSERT INTO bronze_audit VALUES (?,?,?,?,?,?,?)",
                [fp.name, fp.parent.name, datetime.now(timezone.utc),
                 0, datetime.now(timezone.utc), "ERROR", str(exc)]
            )

    con.close()
    logger.info(f"Bronze load complete | {total_rows} total rows inserted")
    return total_rows


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear", action="store_true", help="Wipe bronze before loading")
    args = parser.parse_args()
    n = load_all_raw(clear_first=args.clear)
    print(f"Bronze layer: {n} rows loaded into {DB_PATH}")
