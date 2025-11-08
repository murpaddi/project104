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
import time
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Iterable, Mapping, Sequence, Optional
from datetime import datetime, timezone


_schema = "smartbins"
_archive = f"{_schema}.archive_bin_data"
_static = f"{_schema}.static_bin_data"

# === DATABASE CONNECTION ===
DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("Set DATABASE_URL env variable before running.")

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
    
    cols = [
        "sensor_id", "timestamp", "fill_level_percent", "temperature_c",
        "battery_v", "fill_threshold", "last_emptied", "overflow",
        "overflow_count", "last_overflow"
    ]
    df = df.reindex(columns=cols)

    sql = f"""
        INSERT INTO {_archive} ({", ".join(cols)})
        VALUES ({", ".join([f":{c}" for c in cols])})
        ON CONFLICT (sensor_id, "timestamp") DO NOTHING
        RETURNING 1;
    """

    inserted = 0
    with engine().begin() as conn:
        res = conn.execute(text(sql), df.to_dict(orient="records"))
        try:
            inserted = len(res.fetchall())
        except Exception:
            inserted = res.rowcount or 0

    print(f"DB insert summary: attempted={len(df)} inserted={inserted} (conflicts={len(df)-inserted})")

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
    

def fetch_any_latest_snapshot_df() -> pd.DataFrame:
    """
    Latest row per sensor id with NO time window
    """
    sql = f"""
        SELECT DISTINCT ON (sensor_id) *
        FROM {_archive}
        ORDER BY sensor_id, "timestamp" DESC;
    """
    with engine().begin() as conn:
        return pd.read_sql_query(text(sql), conn)

def fetch_latest_snapshot_df(within_seconds: int = 3600) -> pd.DataFrame:
    """
    Fetches the latest record for each bin_id.
    """

    sql = f"""
        SELECT DISTINCT ON (a.sensor_id) a.*
        FROM {_archive} a
        WHERE a."timestamp" >= (NOW() AT TIME ZONE 'utc') - INTERVAL '{within_seconds} seconds'
        ORDER BY a.sensor_id, a.timestamp DESC;
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

def sync_static_bins(df_coords: pd.DataFrame, *, delete_missing: bool = False, update_existing: bool = False):
    """
    Synchronize static bins with the provided df (bin_id, sensor_id, lat, lng).
    - Inserts new
    - Optionally updates existing coords
    - Optionally deletes static rows not present in df
    """
    if df_coords.empty:
        return
    
    df = df_coords[["bin_id", "sensor_id", "lat", "lng"]].copy()

    eng = engine()
    with eng.begin() as conn:
        conn.exec_driver_sql(f"CREATE TEMP TABLE tmp_bins (LIKE {_static} INCLUDING ALL);")
        df.to_sql("tmp_bins", conn, if_exists="append", index=False)

        conn.exec_driver_sql(f"""
            INSERT INTO {_static} (bin_id, sensor_id, lat, lng)
            SELECT t.bin_id, t.sensor_id, t.lat, t.lng
            FROM tmp_bins t
            LEFT JOIN {_static} s ON s.bin_id = t.bin_id
            WHERE s.bin_id IS NULL;
        """)

        if update_existing:
            conn.exec_driver_sql(f"""
                UPDATE {_static} s
                SET sensor_id = t.sensor_id, lat = t.lat, lng = t.lng
                FROM tmp_bins t
                WHERE s.bin_id = t.bin_id;
        """)
            
        if delete_missing:
            conn.exec_driver_sql(f"""
                DELETE FROM {_static} s
                WHERE NOT EXISTS (SELECT 1 FROM tmp_bins t WHERE t.bin_id = s.bin_id);
        """)
        
        conn.exec_driver_sql("DROP TABLE tmp_bins;")

def ensure_archive_unique_index():
    sql = f"""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_archive_sid_ts
    ON {_archive} (sensor_id, "timestamp");
    """
    with engine().begin() as conn:
        conn.exec_driver_sql(sql)


#Make defunct at later time
def truncate_archive(*, restart_identity: bool = True):
    clause = "RESTART IDENTITY" if restart_identity else ""
    with engine().begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE smartbins.archive_bin_data {clause};")

#Make defunct at later time
def truncate_static():
    with engine().begin() as conn:
        conn.exec_driver_sql("TRUNCATE smartbins.static_bin_data;")


def reset_tables(*, preserve_archive: bool = True, preserve_static: bool = True):
    """
    preserve_archive: True = keeps sensor data intact
    preserve_static: True = keeps static table intact
    """
    with engine().begin() as conn:
        if not preserve_archive:
            conn.exec_driver_sql("TRUNCATE smartbins.archive_bin_data RESTART IDENTITY;")
        if not preserve_static:
            conn.exec_driver_sql("TRUNCATE smartbins.static_bin_data;")


# === WEATHER API ===
_OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
_WEATHER_CACHE_TTL_SEC = 600
_weather_cache: dict[tuple, tuple[float, object]] = {}

def _cache_get(key: tuple):
    now = time.time()
    hit = _weather_cache.get(key)
    if not hit:
        return None
    ts, value = hit
    if (now - ts) > _WEATHER_CACHE_TTL_SEC:
        _weather_cache.pop(key, None)
        return None
    return value

def _cache_put(key: tuple, value: object):
    _weather_cache[key] = (time.time(), value)
    return value


def fetch_weather_now_by_coords(lat: float, lng: float) -> dict:
    key = (round(lat, 4), round(lng, 4), "now", None, None)
    hit = _cache_get(key)
    if hit:
        return hit
    
    params = {
        "latitude": lat,
        "longitude": lng,
        "current": "temperature_2m",
        "timezone": "UTC"
    }
    r = requests.get(_OPEN_METEO_URL, params=params, timeout = 10)
    r.raise_for_status()
    data = r.json()
    cur = data.get("current", {}) or {}
    temp = cur.get("temperature_2m")
    tm = pd.to_datetime(cur.get("time"), utc=True)
    return _cache_put(key, {"temperature_c": (float(temp) if temp is not None else None), "time_utc": tm})

def fetch_weather_now_for_sensors(sensor_ids: list[str] | None = None) -> pd.DataFrame:
    """
    For each of the bins in the static bin table, fetch the weather for the coordinates of that bin.
    Returns DataFrame: ["sensor_id", 'temperature_c", "wx_time_utc"]
    """

    static_df = fetch_static_bins_df()
    if static_df.empty:
        return pd.DataFrame(columns=["sensor_id", "temperature_c", "wx_time_utc"])
    
    if sensor_ids is not None:
        static_df = static_df[static_df["sensor_id"].isin(sensor_ids)]

    if static_df.empty:
        return pd.DataFrame(columns=["sensor_id", "temperature_c", "wx_time_utc"])
    
    coords = static_df[["sensor_id", "lat", "lng"]].dropna()
    uniq = coords[["lat", "lng"]].drop_duplicates()

    #One HTTP call per unique coordinate
    wx_rows = []
    for _, rr in uniq.iterrows():
        w = fetch_weather_now_by_coords(float(rr["lat"]), float(rr["lng"]))
        wx_rows.append({"lat": rr["lat"], "lng": rr["lng"],
                        "temperature_c": w["temperature_c"], "wx_time_utc": w["time_utc"]})
        
    wx = pd.DataFrame(wx_rows)

    out = coords.merge(wx, on=["lat", "lng"], how="left")[["sensor_id", "temperature_c", "wx_time_utc"]]
    return out