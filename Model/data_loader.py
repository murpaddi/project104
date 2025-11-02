#pathlib is now defunct as we are no longer using local csv files for data storage
from pathlib import Path

import pandas as pd
from zoneinfo import ZoneInfo
from typing import Optional, Sequence
from Model import repository as repo

MEL_TZ = ZoneInfo("Australia/Melbourne")

#===HELPER FUNCTIONS===

def _to_melbourne(ts: pd.Series) -> pd.Series:
    s = pd.to_datetime(ts, errors="coerce", utc=True)
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

#=== LOAD LIVE DATA WITH COORDINATES===
    #Latest readings from archive merged with static coordinates
    #Feeds dashboard realtime snapshots
def load_live_with_coords() -> pd.DataFrame:

    live = repo.fetch_latest_snapshot_df()
    coords = repo.fetch_static_bins_df()

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
        validate="one_to_one"
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
    
    return _rename_ui(raw)