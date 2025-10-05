from pathlib import Path
import pandas as pd

ROOT = Path(__file__).parent

RANDOM_DATA_CSV = ROOT / "Random_Bin_Data.csv"
COORDS_CSV = ROOT / "Coordinates.csv"

def read_data() -> pd.DataFrame:
    if not RANDOM_DATA_CSV.exists():
        raise FileNotFoundError(f"Could not find {RANDOM_DATA_CSV}")
    df = pd.read_csv(RANDOM_DATA_CSV)

    for col in ("Fill", "Temp", "Battery"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df 

def read_coords() -> pd.DataFrame:
    if not COORDS_CSV.exists():
        raise FileNotFoundError(f"Could not find {COORDS_CSV}")
    return pd.read_csv(COORDS_CSV)

def load_merged_data() -> pd.DataFrame:
    bins = read_data()
    coords = read_coords()

    if "BinID" not in bins.columns:
        raise KeyError("`BinID` not found in Random_Bin_Data.csv")
    if "BinID" not in coords.columns:
        raise KeyError("`BinID` not found in Coordinates.csv")

    merged = pd.merge(
        bins,
        coords[["BinID", "Lat", "Lng"]],
        on="BinID",                
        how="left"
    )
    merged["HasCoords"] = merged["Lat"].notna() & merged["Lng"].notna() 
    return merged