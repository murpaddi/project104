from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Data"

MASTER_CSV = DATA_DIR / "master_sensor_data.csv"
COORDS_CSV = DATA_DIR / "coordinates.csv"

# inside Model/data_loader.py

def load_live_with_coords() -> pd.DataFrame:
    live = pd.read_csv(MASTER_CSV)
    coords = pd.read_csv(COORDS_CSV)

    # --- Parse timestamp for history ops
    if "timestamp" in live.columns:
        live["timestamp"] = pd.to_datetime(
            live["timestamp"], errors="coerce", utc=True
        ).dt.tz_convert("Australia/Melbourne")


    live = live.sort_values(["sensor_id", "timestamp"])
    live["is_overflow"] = pd.to_numeric(live.get("fill_level_percent"), errors="coerce") >= 98

    # Count only *new* overflow events (rising edge: False → True)
    live["overflow_event"] = live["is_overflow"] & (
        ~live.groupby("sensor_id")["is_overflow"].shift(fill_value=False)
    )

    # Total number of distinct overflow events per bin
    overflow_counts = (
        live.groupby("sensor_id", as_index=False)["overflow_event"]
            .sum()
            .rename(columns={"overflow_event": "Overflow Count"})
    )

    # Timestamp of the most recent overflow *start*
    last_overflow_ts = (
        live[live["overflow_event"]]
            .groupby("sensor_id", as_index=False)["timestamp"]
            .max()
            .rename(columns={"timestamp": "Last Overflow"})
    )

    # 2) Last Emptied from full history (simple heuristic):
    # detect resets: large drop in fill level between consecutive rows for a sensor
    live = live.sort_values(["sensor_id", "timestamp"])
    live["fill_prev"] = live.groupby("sensor_id")["fill_level_percent"].shift(1)
    live["drop"] = pd.to_numeric(live["fill_prev"], errors="coerce") - pd.to_numeric(
        live["fill_level_percent"], errors="coerce"
    )
    # Consider an empty event if drop >= 50 (tweak threshold)
    live["emptied_event"] = live["drop"] >= 50

    last_emptied_ts = (
        live[live["emptied_event"]]
        .groupby("sensor_id", as_index=False)["timestamp"]
        .max()
        .rename(columns={"timestamp": "Last Emptied"})
    )

    # ---------- existing: collapse to latest reading per sensor ----------
    if "timestamp" in live.columns:
        latest = (
            live.sort_values("timestamp")
                .groupby("sensor_id", as_index=False)
                .tail(1)
        )
    else:
        latest = live.drop_duplicates(subset="sensor_id", keep="last")

    # Merge with coordinates
    merged = latest.merge(
        coords[["bin_id", "sensor_id", "lat", "lng"]],
        on="sensor_id",
        how="left"
    )

    # Rename to dashboard names (ensure Fill exists)
    merged.rename(columns={
        "bin_id": "BinID",
        "sensor_id": "DeviceID",
        "lat": "Latitude",
        "lng": "Longitude",
        "temperature_c": "Temp",
        "battery_v": "Battery",
        "fill_level_percent": "Fill",
    }, inplace=True)

    for metric_df in (overflow_counts, last_overflow_ts, last_emptied_ts):
        merged = merged.merge(metric_df, left_on="DeviceID", right_on="sensor_id", how="left")
        merged.drop(columns="sensor_id", inplace=True, errors="ignore")

    merged=merged.loc[:, ~merged.columns.duplicated()]

    if "Overflow Count" not in merged.columns:
        merged["Overflow Count"] = 0
    merged["Overflow Count"] = pd.to_numeric(merged["Overflow Count"], errors = "coerce").fillna(0).astype(int)

    for col in ("Last Overflow", "Last Emptied"):
        if col not in merged.columns:
            merged[col] = pd.NaT

    if "Error Type" not in merged.columns:
        merged["Error Type"] = None

    # Index + save
    merged = merged.set_index("BinID", drop=True)

    merged = merged.drop(
    columns=["overflow", "overflow_count", "last_overflow", "last_emptied",
             "is_overflow", "fill_prev", "drop", "emptied_event"],
    errors="ignore"
)
    merged = merged.loc[:, ~merged.columns.duplicated()]

    if "timestamp" in merged.columns:
        merged.rename(columns={"timestamp": "Timestamp"}, inplace = True)

    merged.to_csv(DATA_DIR / "master_bin_file.csv", index=True)
    return merged
