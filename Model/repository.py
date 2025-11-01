"""
Repository functions for accessing, storing, and updating the data in the database.
Helper functions:

engine() - returns an SQLAlchemy engine connected to the database.

write_archive_rows(rows) - writes multiple rows of bin data to the archive table.

upsert_static_bins(df_coords) - upserts static bin coordinate data to the static_bins_data table.

truncate_archive(*, restart_identity: bool =True) - truncates archive table, optionally restarts id column

truncate_static() - truncates static bin table data.
"""


from __future__ import annotations
import os
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Iterable, Mapping

DB_URL = "postgresql+psycopg2://postgres:GX1YNKqnlbCVPzTi@db.uplrxiyofnwfxnqkhwkm.supabase.co:5432/postgres?sslmode=require"


_engine = None
def engine():
    global _engine
    if _engine is None:
        _engine = create_engine(DB_URL, pool_pre_ping=True)
    return _engine

def write_archive_rows(rows: Iterable[Mapping]):
    df = pd.DataFrame(list(rows))
    if df.empty:
        return
    df.to_sql(
        "archive_bin_data",
        engine(),
        schema="smartbins",
        if_exists="append",
        index=False
    )

def upsert_static_bins(df_coords: pd.DataFrame):
    df = df_coords[["bin_id", "sensor_id", "lat", "lng"]].copy()
    eng = engine()
    with eng.begin() as conn:
        conn.exec_driver_sql("CREATE TEMP TABLE tmp_bins (LIKE smartbins.static_bin_data INCLUDING ALL);")
        df.to_sql("tmp_bins", conn, if_exists="append", index=False)
        conn.exec_driver_sql("""
            INSERT INTO smartbins.static_bin_data (bin_id, sensor_id, lat, lng)
            SELECT bin_id, sensor_id, lat, lng FROM tmp_bins
            ON CONFLICT (bin_id) DO UPDATE
            SET sensor_id = EXCLUDED.sensor_id,
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng;
            DROP TABLE tmp_bins;
        """)

#Make defunct at later time
def truncate_archive(*, restart_identity: bool = True):
    clause = "RESTART IDENTITY" if restart_identity else ""
    with engine().begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE smartbins.archive_bin_data {clause};")

#Make defunct at later time
def truncate_static():
    with engine().begin() as conn:
        conn.exec_driver_sql("TRUNCATE smartbins.static_bin_data;")

#Make defunct at later time
def reset_all(*, preserve_static: bool = True):
    with engine().begin() as conn:
        conn.exec_driver_sql("TRUNCATE smartbins.archive_bin_data RESTART IDENTITY;")
        if not preserve_static:
            conn.exec_driver_sql("TRUNCATE smartbins.static_bin_data;")