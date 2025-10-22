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
        live["timestamp"] = pd.to_datetime(live["timestamp"], errors="coerce", utc=True).dt.tz_convert("Australia/Melbourne")

    # ---------- derive history-based metrics ----------
    # 1) Overflow metrics from full history
    # Define overflow as fill >= 100 (tweak if you prefer >= 95/98/etc.)
    live["is_overflow"] = pd.to_numeric(live.get("fill_level_percent"), errors="coerce") >= 98

    overflow_counts = (
        live.groupby("sensor_id", as_index=False)["is_overflow"]
            .sum()
            .rename(columns={"is_overflow": "Overflow Count"})
    )

    # Last Overflow timestamp (NaT if none)
    last_overflow_ts = (
        live[live["is_overflow"]]
        .sort_values("timestamp")
        .groupby("sensor_id", as_index=False)
        .tail(1)[["sensor_id", "timestamp"]]
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

    # ---------- attach history metrics to the latest snapshot ----------
    merged = merged.merge(
        overflow_counts, left_on="DeviceID", right_on="sensor_id", how="left"
    ).merge(
        last_overflow_ts, left_on="DeviceID", right_on="sensor_id", how="left"
    ).merge(
        last_emptied_ts, left_on="DeviceID", right_on="sensor_id", how="left"
    )

    # Clean up any helper columns that may linger after the merges
    for col in ("sensor_id_x", "sensor_id_y", "sensor_id"):
        if col in merged.columns:
            merged.drop(columns=col, inplace=True)
    # If metrics missing, fill sensible defaults
    if "Overflow Count" not in merged.columns:
        merged["Overflow Count"] = 0
    merged["Overflow Count"] = merged["Overflow Count"].fillna(0).astype(int)
    if "Last Overflow" not in merged.columns:
        merged["Last Overflow"] = pd.NaT
    if "Last Emptied" not in merged.columns:
        merged["Last Emptied"] = pd.NaT

    # You can still add Error Type later in Utilities.filter_urgent; keep None here
    if "Error Type" not in merged.columns:
        merged["Error Type"] = None

    # Index + save
    merged = merged.set_index("BinID", drop=True)
    merged.to_csv(DATA_DIR / "master_bin_file.csv", index=True)
    return merged
