"""
STEP 6 — PIPELINE ORCHESTRATOR
Runs the full pipeline: Ingest → Bronze → Silver → Gold
Can run once or continuously on a schedule.
Usage:
  python pipeline/run_pipeline.py --once
  python pipeline/run_pipeline.py         (continuous, runs every 60s)
"""
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import load_config, setup_logger, ensure_dirs

from ingestion.ingest import run_once as ingest_once
from bronze.load_bronze import load_all_raw
from silver.process_silver import run_silver
from gold.build_gold import run_gold

setup_logger("pipeline")
from loguru import logger

cfg = load_config()


def run_pipeline(clear_layers: bool = False):
    """Execute one full pipeline cycle."""
    start = datetime.now(timezone.utc)
    logger.info("════════════════════════════════════════")
    logger.info(f"Pipeline started at {start.isoformat()}")

    try:
        # 1. Ingest
        logger.info("STEP 1/4 ── Ingestion")
        counts = ingest_once()
        logger.info(f"Ingested: {counts}")

        # 2. Bronze
        logger.info("STEP 2/4 ── Bronze layer")
        n_bronze = load_all_raw(clear_first=clear_layers)
        logger.info(f"Bronze rows: {n_bronze}")

        # 3. Silver
        logger.info("STEP 3/4 ── Silver layer (clean + CDC + SCD2)")
        run_silver(clear_first=clear_layers)

        # 4. Gold
        logger.info("STEP 4/4 ── Gold layer (dimensions + KPIs)")
        run_gold(clear_first=clear_layers)

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"Pipeline complete in {elapsed:.1f}s")
        return True

    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def run_continuous(interval: int = None):
    """Run pipeline on a schedule forever."""
    interval = interval or cfg["api"]["poll_interval"]
    ensure_dirs()
    cycle = 0
    logger.info(f"Continuous pipeline — interval={interval}s")
    while True:
        cycle += 1
        logger.info(f"\n{'═'*40}\nCYCLE {cycle}\n{'═'*40}")
        # Only clear on cycle 1 if requested; after that always append
        success = run_pipeline(clear_layers=False)
        status  = "✓" if success else "✗"
        logger.info(f"Cycle {cycle} {status} — sleeping {interval}s")
        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flight Tracker Pipeline")
    parser.add_argument("--once",    action="store_true", help="Run one cycle and exit")
    parser.add_argument("--clear",   action="store_true", help="Clear all layers before running")
    parser.add_argument("--interval",type=int, default=None, help="Override poll interval (seconds)")
    args = parser.parse_args()

    ensure_dirs()

    if args.once:
        ok = run_pipeline(clear_layers=args.clear)
        sys.exit(0 if ok else 1)
    else:
        run_continuous(interval=args.interval)
