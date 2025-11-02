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
from typing import Iterable, Mapping, Sequence, Optional
from datetime import datetime, timedelta, timezone

DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("Set DATABASE_URL env variable before running.")

_schema = "smartbins"
_archive = f"{_schema}.archive_bin_data"
_static = f"{_schema}.static_bin_data"


_engine = None
def engine():
    global _engine
    if _engine is None:
        _engine = create_engine(DB_URL, pool_pre_ping=True)
    return _engine

# === WRITE HELPERS ===
def write_archive_rows(rows: Iterable[Mapping]):
    df = pd.DataFrame(list(rows))
    if df.empty:
        return
    df.to_sql(
        "archive_bin_data",
        engine(),
        schema=_schema,
        if_exists="append",
        index=False
    )

def upsert_static_bins(df_coords: pd.DataFrame):
    df = df_coords[["bin_id", "sensor_id", "lat", "lng"]].copy()
    eng = engine()
    with eng.begin() as conn:
        conn.exec_driver_sql(f"CREATE TEMP TABLE tmp_bins (LIKE {_static} INCLUDING ALL);")
        df.to_sql("tmp_bins", conn, if_exists="append", index=False)
        conn.exec_driver_sql(f"""
            INSERT INTO {_static} (bin_id, sensor_id, lat, lng)
            SELECT bin_id, sensor_id, lat, lng FROM tmp_bins
            ON CONFLICT (bin_id) DO UPDATE
            SET sensor_id = EXCLUDED.sensor_id,
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng;
            DROP TABLE tmp_bins;
        """)

# === READ HELPERS ===
def fetch_archive_df(
    *,
    since: Optional[datetime | str] = None,
    until: Optional[datetime | str] = None,
    sensor_ids: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    columns: str = "*"
) -> pd.DataFrame:
    """
    Fetches the rows from the archive table as a DataFrame.

    Behaviour:
    - If no filters are provided, returns the entire table.
    - If since/until are provided, applies a time range filter (UTC).
    - If sensor_ids are provided, filters by those ids.
    - Results are ordered by bin_id and timestamp.

    Args:
        since: Optional datetime or ISO string to filter rows from (inclusive).
        until: Optional datetime or ISO string to filter rows until (exclusive).
        sensor_ids: Optional list of sensor ids to filter by.
        limit: Optional maximum number of rows to return
        columns: SQL select list (default "*").

    Returns:
        Pandas DataFrame with raw DB column names.
    """

    where_clauses: list[str] = []
    params: dict[str, object] = {}

    #Time bounds (optional)
    if since is not None:
        if isinstance(since, datetime):
            since = since.astimezone(timezone.utc) if since.tzinfo else since.replace(tzinfo=timezone.utc)
            params["since"] = since
        else:
            params["since"] = pd.to_datetime(since, utc = True).to_pydatetime()
        where_clauses.append("timestamp >= :since")
    
    if until is not None:
        if isinstance(until, datetime):
            until = until.astimezone(timezone.utc) if until.tzinfo else until.replace(tzinfo=timezone.utc)
            params["until"] = until
        else:
            params["until"] = pd.to_datetime(until, utc = True).to_pydatetime()
        where_clauses.append("timestamp < :until")
    
    #Sensor filter (optional)
    if sensor_ids:
        where_clauses.append("sensor_id = ANY(:sensor_ids)")
        params["sensor_ids"] = list(sensor_ids)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    lim_sql = f" LIMIT {int(limit)}" if limit else ""
    order_sql = "ORDER BY sensor_id, timestamp"

    sql = f"""
        SELECT {columns}
        FROM {_archive}
        {where_sql}
        {order_sql}
        {lim_sql};
    """

    with engine().begin() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)
    

def fetch_latest_snapshot_df() -> pd.DataFrame:
    """
    Fetches the latest record for each bin_id.
    """

    sql = f"""
        SELECT DISTINCT ON (sensor_id) *
        FROM {_archive}
        ORDER BY sensor_id, timestamp DESC;
    """
    with engine().begin() as conn:
        df = pd.read_sql_query(text(sql), conn)
    return df

def fetch_static_bins_df() -> pd.DataFrame:
    """
    Fetches static bin coordinates.
    """

    sql = f"SELECT * FROM {_static}"
    with engine().begin() as conn:
        df = pd.read_sql_query(text(sql), conn)
    return df
    
# === ADMIN HELPERS ===
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