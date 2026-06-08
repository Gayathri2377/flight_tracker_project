"""
STEP 1 — INGESTION LAYER
Polls ADSB.lol API every 60 seconds across multiple regions.
Saves raw JSON files to data/raw/ with timestamps and region metadata.
"""
import json
import time
import sys
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import requests
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from loguru import logger

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import load_config, setup_logger, ensure_dirs, BASE_DIR

setup_logger("ingestion")
cfg = load_config()

API_BASE   = cfg["api"]["base_url"]
TIMEOUT    = cfg["api"]["timeout"]
RETRIES    = cfg["api"]["retry_attempts"]
RETRY_WAIT = cfg["api"]["retry_delay"]
POLL_SECS  = cfg["api"]["poll_interval"]
RAW_DIR    = BASE_DIR / cfg["paths"]["raw_data"]


# ── helpers ──────────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(RETRIES),
    wait=wait_fixed(RETRY_WAIT),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def _call_api(url: str) -> dict:
    resp = requests.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_region(region_key: str, region_cfg: dict) -> Optional[dict]:
    """Fetch aircraft for one region and return enriched payload."""
    lat  = region_cfg["lat"]
    lon  = region_cfg["lon"]
    dist = region_cfg["dist_nm"]
    url  = f"{API_BASE}/lat/{lat}/lon/{lon}/dist/{dist}"

    ingested_at = datetime.now(timezone.utc).isoformat()
    try:
        data = _call_api(url)
    except Exception as exc:
        logger.error(f"[{region_key}] API call failed after {RETRIES} retries: {exc}")
        return None

    aircraft = data.get("ac", [])

    # Enrich every aircraft record with metadata
    for ac in aircraft:
        ac["_region"]      = region_key
        ac["_region_label"]= region_cfg["label"]
        ac["_ingested_at"] = ingested_at
        ac["_source_url"]  = url

    payload = {
        "region":       region_key,
        "region_label": region_cfg["label"],
        "ingested_at":  ingested_at,
        "api_url":      url,
        "aircraft_count": len(aircraft),
        "aircraft":     aircraft,
    }
    logger.info(f"[{region_key}] Fetched {len(aircraft)} aircraft")
    return payload


def save_raw(payload: dict, region_key: str):
    """Persist one region's raw payload as a timestamped JSON file."""
    ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = RAW_DIR / region_key
    path.mkdir(parents=True, exist_ok=True)
    file = path / f"{region_key}_{ts}.json"
    with open(file, "w") as fh:
        json.dump(payload, fh, indent=2)
    logger.info(f"[{region_key}] Saved → {file.name}")


def run_once():
    """Single poll cycle — all configured regions."""
    regions = cfg["regions"]
    results = {}
    for key, rcfg in regions.items():
        payload = fetch_region(key, rcfg)
        if payload:
            save_raw(payload, key)
            results[key] = payload["aircraft_count"]
    return results


def run_continuous():
    """Continuous polling loop."""
    logger.info(f"Starting continuous ingestion (interval={POLL_SECS}s)")
    ensure_dirs()
    cycle = 0
    while True:
        cycle += 1
        logger.info(f"── Cycle {cycle} ──────────────────────────")
        try:
            counts = run_once()
            total  = sum(counts.values())
            logger.info(f"Cycle {cycle} complete | total aircraft={total} | {counts}")
        except Exception as exc:
            logger.error(f"Cycle {cycle} error: {exc}")
        time.sleep(POLL_SECS)


# ── entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ADSB.lol ingestion")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    args = parser.parse_args()

    ensure_dirs()
    if args.once:
        counts = run_once()
        print(f"Done: {counts}")
    else:
        run_continuous()
