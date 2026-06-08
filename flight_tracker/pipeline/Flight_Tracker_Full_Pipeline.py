# Flight Tracker Full Pipeline - Ready for GitHub & Databricks

import duckdb
import yaml
from pathlib import Path
from loguru import logger
import pandas as pd

# ========================= CONFIG =========================
BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR / "data"

# Create folders
for folder in ["raw", "bronze", "silver", "gold", "warehouse", "logs"]:
    (DATA_ROOT / folder).mkdir(parents=True, exist_ok=True)

# Connect to DuckDB
db_path = DATA_ROOT / "warehouse" / "flights.duckdb"
con = duckdb.connect(str(db_path))
print(f"✅ Connected to: {db_path}")

# ====================== FUNCTIONS ======================

def show_layer(layer: str):
    print(f"\n🔹 {layer.upper()} LAYER:")
    try:
        tables = con.sql(f"SHOW TABLES LIKE '{layer}.%'").df()
        if tables.empty:
            print("   No tables found.")
            return
        print(tables)
        for tbl in tables['name']:
            count = con.sql(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"   → {tbl}: {count:,} rows")
            print(con.sql(f"SELECT * FROM {tbl} LIMIT 10").df())
    except Exception as e:
        print(f"   Error: {e}")

def clear_layer(layer: str, confirm=False):
    if not confirm:
        print(f"⚠️  To clear {layer}, use confirm=True")
        return
    tables = con.sql(f"SHOW TABLES LIKE '{layer}.%'").df()
    for tbl in tables['name']:
        con.sql(f"DROP TABLE IF EXISTS {tbl}")
        print(f"🗑️ Dropped: {tbl}")
    print(f"✅ {layer.upper()} layer cleared!")

# ====================== RUN ======================

print("📋 ALL TABLES:")
print(con.sql("SHOW TABLES").df())

show_layer("silver")
show_layer("gold")

# --- To clear data, uncomment below ---
# clear_layer("silver", confirm=True)
# clear_layer("gold", confirm=True)

con.close()
print("\n✅ Script finished!")