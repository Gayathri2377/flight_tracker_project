"""
Flight Tracker - Check Silver & Gold Layers
Run this script to see your data in Silver and Gold layers.
"""

import duckdb
from pathlib import Path
from flight_tracker.config.settings import CONFIG, ensure_dirs

def print_table(con, table_name: str, limit: int = 20):
    """Print table contents nicely"""
    print(f"\n{'='*80}")
    print(f"📊 TABLE: {table_name.upper()}")
    print(f"{'='*80}")
    
    try:
        # Get row count
        count = con.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Total rows: {count:,}\n")
        
        # Print sample rows
        df = con.sql(f"SELECT * FROM {table_name} LIMIT {limit}").df()
        print(df.to_string(index=False))
        
        if count > limit:
            print(f"\n... showing first {limit} rows out of {count:,} total rows.")
            
    except Exception as e:
        print(f"❌ Error reading table '{table_name}': {e}")


def main():
    ensure_dirs()  # Make sure folders exist
    
    db_path = CONFIG['paths']['duckdb']
    print(f"🔗 Connecting to DuckDB: {db_path}\n")
    
    con = duckdb.connect(str(db_path))
    
    # Check Silver layer
    print_table(con, "silver.flights")      # Common table name
    # print_table(con, "silver.flight_status")  # Uncomment if you have this table
    
    # Check Gold layer
    print_table(con, "gold.flights_summary")   # Common table name
    # print_table(con, "gold.daily_stats")       # Uncomment if exists
    
    # List all tables available
    print(f"\n{'='*80}")
    print("📋 ALL TABLES IN DATABASE:")
    print(f"{'='*80}")
    tables = con.sql("SHOW TABLES").df()
    print(tables)
    
    con.close()
    print("\n✅ Done!")


if __name__ == "__main__":
    main()