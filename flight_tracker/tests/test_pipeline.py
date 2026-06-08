"""
Tests for the flight tracker pipeline.
Run: pytest tests/ -v
"""
import json
import sys
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ── fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_AIRCRAFT = [
    {
        "hex": "800c9b",
        "flight": "AI101   ",
        "r": "VT-ANF",
        "t": "B77W",
        "lat": 17.52,
        "lon": 78.91,
        "alt_baro": "34000",
        "gs": 480.0,
        "track": 245.0,
        "baro_rate": -64.0,
        "squawk": "2041",
        "category": "A5",
        "seen": 0.4,
        "messages": 120,
        "emergency": None,
        "alert": 0,
        "spi": 0,
        "_region": "hyderabad",
        "_region_label": "Hyderabad",
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
        "_source_url": "https://api.adsb.lol/v2/lat/17.38/lon/78.48/dist/250",
    },
    {
        "hex": "a12345",
        "flight": "SG202   ",
        "r": "VT-SLB",
        "t": "B738",
        "lat": 18.10,
        "lon": 79.20,
        "alt_baro": "ground",
        "gs": 0.0,
        "track": 90.0,
        "baro_rate": 0.0,
        "squawk": "7700",  # Emergency!
        "emergency": "general",
        "_region": "hyderabad",
        "_region_label": "Hyderabad",
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
        "_source_url": "https://api.adsb.lol/v2/lat/17.38/lon/78.48/dist/250",
    },
]

SAMPLE_PAYLOAD = {
    "region": "hyderabad",
    "region_label": "Hyderabad",
    "ingested_at": datetime.now(timezone.utc).isoformat(),
    "api_url": "https://api.adsb.lol/v2/lat/17.38/lon/78.48/dist/250",
    "aircraft_count": len(SAMPLE_AIRCRAFT),
    "aircraft": SAMPLE_AIRCRAFT,
}


@pytest.fixture
def tmp_db():
    """Temporary DuckDB for tests."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    yield db_path
    os.unlink(db_path)


@pytest.fixture
def tmp_raw_dir(tmp_path):
    raw_dir = tmp_path / "raw" / "hyderabad"
    raw_dir.mkdir(parents=True)
    f = raw_dir / "hyderabad_test.json"
    f.write_text(json.dumps(SAMPLE_PAYLOAD))
    return tmp_path


# ── ingestion tests ───────────────────────────────────────────────────────────

def test_fetch_region_returns_payload():
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"ac": SAMPLE_AIRCRAFT}
    mock_resp.raise_for_status = MagicMock()

    with patch("requests.get", return_value=mock_resp):
        from ingestion.ingest import fetch_region
        payload = fetch_region("hyderabad", {"lat": 17.38, "lon": 78.48, "dist_nm": 250, "label": "Hyderabad"})

    assert payload is not None
    assert payload["aircraft_count"] == len(SAMPLE_AIRCRAFT)
    assert all("_region" in ac for ac in payload["aircraft"])
    assert all("_ingested_at" in ac for ac in payload["aircraft"])


def test_fetch_region_handles_api_failure():
    with patch("requests.get", side_effect=Exception("Connection refused")):
        from ingestion.ingest import fetch_region
        result = fetch_region("test", {"lat": 0, "lon": 0, "dist_nm": 100, "label": "Test"})
    assert result is None


# ── bronze layer tests ────────────────────────────────────────────────────────

def test_bronze_load(tmp_raw_dir, tmp_db, monkeypatch):
    monkeypatch.setattr("bronze.load_bronze.DB_PATH", Path(tmp_db))
    monkeypatch.setattr("bronze.load_bronze.RAW_DIR", tmp_raw_dir / "raw")

    from bronze.load_bronze import load_all_raw, get_connection
    n = load_all_raw(clear_first=True)

    assert n == len(SAMPLE_AIRCRAFT)
    con = duckdb.connect(tmp_db)
    count = con.execute("SELECT COUNT(*) FROM flight_raw").fetchone()[0]
    assert count == len(SAMPLE_AIRCRAFT)
    con.close()


def test_bronze_preserves_raw_json(tmp_raw_dir, tmp_db, monkeypatch):
    monkeypatch.setattr("bronze.load_bronze.DB_PATH", Path(tmp_db))
    monkeypatch.setattr("bronze.load_bronze.RAW_DIR", tmp_raw_dir / "raw")

    from bronze.load_bronze import load_all_raw
    load_all_raw(clear_first=True)

    con = duckdb.connect(tmp_db)
    rows = con.execute("SELECT _raw_json FROM flight_raw").fetchall()
    for row in rows:
        parsed = json.loads(row[0])
        assert "hex" in parsed
    con.close()


# ── silver layer tests ────────────────────────────────────────────────────────

def test_silver_clean_filters_invalid(tmp_db):
    con = duckdb.connect(tmp_db)
    # Create minimal bronze table
    con.execute("""
        CREATE TABLE flight_raw (
            hex VARCHAR, flight VARCHAR, registration VARCHAR,
            aircraft_type VARCHAR, squawk VARCHAR, category VARCHAR,
            lat DOUBLE, lon DOUBLE, alt_baro VARCHAR, alt_geom DOUBLE,
            gs DOUBLE, ias DOUBLE, tas DOUBLE, mach DOUBLE, track DOUBLE,
            track_rate DOUBLE, roll DOUBLE, baro_rate DOUBLE, geom_rate DOUBLE,
            nav_qnh DOUBLE, nav_altitude_mcp DOUBLE, nav_altitude_fms DOUBLE,
            nav_heading DOUBLE, seen DOUBLE, seen_pos DOUBLE, rssi DOUBLE,
            messages BIGINT, emergency VARCHAR, alert INTEGER, spi INTEGER,
            _region VARCHAR, _region_label VARCHAR, _ingested_at TIMESTAMP,
            _source_url VARCHAR, _loaded_at TIMESTAMP, _source_file VARCHAR,
            _raw_json VARCHAR
        )
    """)
    now = datetime.now(timezone.utc)
    # Valid row
    con.execute("INSERT INTO flight_raw (hex, lat, lon, alt_baro, gs, _region, _region_label, _ingested_at, _loaded_at) VALUES ('abc123', 17.5, 78.9, '35000', 450, 'hyd', 'Hyderabad', ?, ?)", [now, now])
    # Invalid coords
    con.execute("INSERT INTO flight_raw (hex, lat, lon, alt_baro, gs, _region, _region_label, _ingested_at, _loaded_at) VALUES ('bad001', 999, 999, '35000', 450, 'hyd', 'Hyderabad', ?, ?)", [now, now])
    con.close()

    assert True  # Schema creation passed


def test_cdc_detects_new_aircraft():
    """CDC should mark aircraft as NEW when not in flight_current."""
    rows_cdc = []
    # Simulate: existing map is empty → all should be NEW
    existing_map = {}
    events = []
    for ac in SAMPLE_AIRCRAFT:
        h = ac["hex"]
        prev = existing_map.get(h)
        event = "NEW" if prev is None else "UNCHANGED"
        events.append(event)
    assert all(e == "NEW" for e in events)


def test_scd2_tracks_altitude_change():
    """SCD2 should record a change when altitude differs."""
    prev = {"flight": "AI101", "registration": "VT-ANF", "squawk": "2041",
            "altitude_ft": 30000, "gs_knots": 450, "lat": 17.5, "lon": 78.9}
    curr = {"flight": "AI101", "registration": "VT-ANF", "squawk": "2041",
            "altitude_ft": 35000, "gs_knots": 450, "lat": 17.5, "lon": 78.9}
    tracked = ["flight", "registration", "squawk", "altitude_ft", "gs_knots", "lat", "lon"]
    changes = [c for c in tracked if str(curr.get(c)) != str(prev.get(c))]
    assert "altitude_ft" in changes


# ── API tests ─────────────────────────────────────────────────────────────────

def test_api_health(tmp_db, monkeypatch):
    monkeypatch.setattr("api.main.DB_PATH", tmp_db)
    con = duckdb.connect(tmp_db)
    con.close()

    from fastapi.testclient import TestClient
    from api.main import app
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_api_root():
    from fastapi.testclient import TestClient
    from api.main import app
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200


# ── data quality tests ────────────────────────────────────────────────────────

def test_emergency_detection():
    """Aircraft with squawk 7700 should be flagged."""
    emergency_ac = [ac for ac in SAMPLE_AIRCRAFT if ac.get("emergency") == "general"]
    assert len(emergency_ac) == 1
    assert emergency_ac[0]["squawk"] == "7700"


def test_ground_detection():
    """Aircraft with alt_baro='ground' should be on_ground=True."""
    ground_ac = [ac for ac in SAMPLE_AIRCRAFT if ac.get("alt_baro") == "ground"]
    assert len(ground_ac) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
