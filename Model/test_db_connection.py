from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
import pandas as pd

import repository as repo

def _print_df(title: str, df: pd.DataFrame, *, n: int = 5):
    print(f"\n=== {title} ===")
    if df.empty:
        print("No rows returned.")
        return
    print(f"Rows: {len(df):,}  |  Cols: {len(df.columns)}  |  First {n} rows:")
    print(df.head(n).to_string(index=False))

def main():

    if not os.environ.get("DATABASE_URL"):
        raise SystemExit("Set DATABASE_URL env variable before running.")
    
    #Basic connection test
    try:
        with repo.engine().connect() as conn:
            conn.exec_driver_sql("SELECT 1;")
        print("Database connection OK.")
    except Exception as e:
        raise SystemExit(f"Database connection ERROR: {e}")
    
    #Lastest snapshot per bin
    try:
        latest = repo.fetch_latest_snapshot_df()
        _print_df("Latest snapshot per bin", latest)
        if not latest.empty:
            print("Sensors:", ", ".join(map(str, latest["sensor_id"].unique())))
    except Exception as e:
        print(f"Error fetching latest snapshot per bin: {e}")

    #Entire archive (no filters)
    try:
        archive_all = repo.fetch_archive_df(limit=200)
        _print_df("Archive sample (ordered by sensor_id, timestamp)", archive_all)
    except Exception as e:
        print(f"Error fetching entire archive: {e}")

    #Between dates
    try:
        now_utc = datetime.now(timezone.utc)
        since_6h = now_utc - timedelta(hours=6)
        recent = repo.fetch_archive_df(since=since_6h, until=now_utc)
        _print_df("Archive (last 6 hours)", recent)
    except Exception as e:
        print(f"Error fetching archive from last 6 hours: {e}")

    #Filter by sensor ID
    try:
        example_sensors = ["R718X-001"]
        sel = repo.fetch_archive_df(sensor_ids=example_sensors, limit=200)
        _print_df(f"Archive (sensor_ids={example_sensors})", sel)
    except Exception as e:
        print(f"Error fetching archive for sensor_ids={example_sensors}: {e}")
    
    print("\nDone.")

if __name__ == "__main__":
    main()