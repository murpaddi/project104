import pandas as pd
from zoneinfo import ZoneInfo
from typing import Optional, Sequence
from Model import repository as repo

MEL_TZ = ZoneInfo("Australia/Melbourne")

# ===HELPER FUNCTIONS===

def _to_melbourne(ts) -> pd.Series:
    s = pd.to_datetime(ts, errors="coerce", utc=True)
    if not isinstance(s, pd.Series):
        s=pd.Series(s)
    return s.dt.tz_convert(MEL_TZ)

def _numeric(df: pd.DataFrame, cols: Sequence[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def _rename_ui(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "bin_id": "BinID",
            "sensor_id": "DeviceID",
            "timestamp": "Timestamp",
            "fill_level_percent": "Fill",
            "temperature_c": "Temperature",
            "battery_v": "Battery",
            "lat": "Latitude",
            "lng": "Longitude",
            "overflow_count": "Overflow #",
            "last_overflow": "Last Overflow",
            "last_emptied": "Last Emptied"
        }
    )

def _finalise(df: pd.DataFrame) -> pd.DataFrame:
    if "Fill" in df.columns:
        df["Fill"] = pd.to_numeric(df["Fill"], errors="coerce").clip(0, 100)

    preferred = [
        "BinID", "DeviceID", "Timestamp",
        "Fill", "Temperature", "Battery",
        "Latitude", "Longitude",
        "Overflow #", "Last Overflow", "Last Emptied",
        "fill_threshold"
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df.loc[:, pd.Index(cols).drop_duplicates()]

    if "BinID" in df.columns and df["BinID"].notna().any():
        df = df.set_index("BinID", drop=True)
    elif "DeviceID" in df.columns:
        df = df.set_index("DeviceID", drop=True)
    return df

# === WEATHER API HELPERS ===
def fetch_bins_weather_now() -> pd.DataFrame:
    """
    Per-sensor weather using static coordinates.
    Returns [sensor_id, temperature_c, wx_time_utc]
    """
    static_df = repo.fetch_static_bins_df()
    if static_df.empty:
        return pd.DataFrame(columns=["sensor_id", "temperature_c", "wx_time_utc"])
    sensor_ids = static_df["sensor_id"].dropna().unique().tolist()
    return repo.fetch_weather_now_for_sensors(sensor_ids=sensor_ids)

def load_weather_now_with_coords() -> pd.DataFrame:
    coords = repo.fetch_static_bins_df()
    if coords.empty:
        return pd.DataFrame(columns=[
            "BinID", "DeviceID", "Temperature", "Latitude", "Longitude", "Timestamp"
        ]).set_index("BinID", drop=True)
    
    wx = repo.fetch_weather_now_for_sensors(sensor_ids=coords["sensor_id"].dropna().unique().tolist())
    df = coords.merge(wx, on="sensor_id", how="left")
    df = df.rename(columns={
        "bin_id": "BinID",
        "sensor_id": "DeviceID",
        "lat": "Latitude",
        "lng": "Longitude",
        "temperature_c": "Temperature",
        "wx_time_utc": "Timestamp"
    })

    if "Timestamp" in df.columns:
        df["Timestamp"] = _to_melbourne(df["Timestamp"])
    
    return _finalise(df)

#=== LOAD LIVE DATA WITH COORDINATES===
    #Latest readings from archive merged with static coordinates
    #Feeds dashboard realtime snapshots
def load_live_with_coords() -> pd.DataFrame:

    live = repo.fetch_latest_snapshot_df()
    coords = repo.fetch_static_bins_df()

    if live is None or live.empty:
        df = coords[["bin_id","sensor_id","lat","lng"]].copy() if not coords.empty else pd.DataFrame()
        df = _rename_ui(df)
        return _finalise(df)
    
    need_temp = ("temperature_c" not in live.columns) or (live.get("temperature_c").isna().all())

    if need_temp and not coords.empty and "sensor_id" in live.columns:
        wx = repo.fetch_weather_now_for_sensors(sensor_ids=live["sensor_id"].dropna().unique().tolist())
        if not wx.empty:
            live = live.merge(wx, on="sensor_id", how="left")
            # normalise to a single 'temperature_c' column
            if "temperature_c_x" in live.columns and "temperature_c_y" in live.columns:
                live["temperature_c"] = live["temperature_c_x"].combine_first(live["temperature_c_y"])
                live.drop(columns=["temperature_c_x", "temperature_c_y"], inplace=True)
            # wx_time_utc is informational; drop for UI
            live.drop(columns=["wx_time_utc"], errors="ignore", inplace=True)       

    #Converting timestamps to Melbourne time
    if "timestamp" in live.columns:
        live["timestamp"] = _to_melbourne(live["timestamp"])

    _numeric(live, ["fill_level_percent", "temperature_c", "battery_v",
                    "fill_threshold", "overflow_count"])
    
    if "last_overflow" in live.columns:
        live["last_overflow"] = _to_melbourne(live["last_overflow"])
    if "last_emptied" in live.columns:
        live["last_emptied"] = _to_melbourne(live["last_emptied"])

    #merge static coordinates onto live data snapshot
    df = live.merge(
        coords[["bin_id", "sensor_id", "lat", "lng"]],
        on="sensor_id",
        how="left",
        validate="many_to_one"
    )

    #rename columns for UI
    df = _rename_ui(df)
    return _finalise(df)

def load_archive_with_coords(
    device_id: str,
    *,
    since: Optional[str | pd.Timestamp] = None,
    until: Optional[str | pd.Timestamp] = None,
    limit: Optional[int] = None,
    with_coords: bool = False,
) -> pd.DataFrame:
    """time series data from the archive table for downloads and analytics"""
    raw = repo.fetch_archive_df(
        since=since,
        until=until,
        sensor_ids=[device_id],
        limit=limit
    )
    if raw.empty:
        return raw
    
    #Types
    raw["timestamp"] = _to_melbourne(raw["timestamp"])

    _numeric(raw, ["fill_level_percent", "temperature_c", "battery_v",
                   "fill_threshold", "overflow_count"])
    
    if "last_overflow" in raw.columns:
        raw["last_overflow"] = _to_melbourne(raw["last_overflow"])

    if "last_emptied" in raw.columns:
        raw["last_emptied"] = _to_melbourne(raw["last_emptied"])
    
    if with_coords:
        coords = repo.fetch_static_bins_df()
        raw = raw.merge(
            coords[["bin_id", "sensor_id", "lat", "lng"]],
            on="sensor_id",
            how="left",
            validate="many_to_one"
        )
    
    out = _rename_ui(raw)
    return _finalise(out)