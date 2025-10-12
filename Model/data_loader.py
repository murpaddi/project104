from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Data"

MASTER_CSV = DATA_DIR / "master_sensor_data.csv"
COORDS_CSV = DATA_DIR / "coordinates.csv"

def load_live_with_coords() -> pd.DataFrame:
    live = pd.read_csv(MASTER_CSV)
    coords = pd.read_csv(COORDS_CSV)
    merged = live.merge(
        coords[["bin_id", "sensor_id", "lat", "lng"]],
        on="sensor_id",
        how = "right"
    )

    merged.rename(columns={
        "bin_id": "BinID",
        "sensor_id": "DeviceID",
        "lat": "Latitude",
        "lng": "Longitude",
        "temperature_c": "Temp",
        "battery_v": "Battery"
    }, inplace=True)
    merged = merged.set_index("bin_id", drop=True)
    return merged