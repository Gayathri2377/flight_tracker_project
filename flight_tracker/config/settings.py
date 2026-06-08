"""Shared configuration and utilities for the flight tracker project."""
import os
import yaml
from pathlib import Path
from loguru import logger

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "config.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def setup_logger(log_name: str = "flight_tracker"):
    cfg = load_config()
    log_dir = BASE_DIR / cfg["paths"]["logs"]
    log_dir.mkdir(parents=True, exist_ok=True)
    logger.add(
        log_dir / f"{log_name}.log",
        rotation=cfg["logging"]["rotation"],
        retention=cfg["logging"]["retention"],
        level=cfg["logging"]["level"],
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}",
    )
    return logger


def ensure_dirs():
    cfg = load_config()
    for key in ["raw_data", "bronze", "silver", "gold"]:
        path = BASE_DIR / cfg["paths"][key]
        path.mkdir(parents=True, exist_ok=True)
    duckdb_path = BASE_DIR / cfg["paths"]["duckdb"]
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)


CONFIG = load_config()
