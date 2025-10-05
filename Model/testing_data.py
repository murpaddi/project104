import pandas as pd
from Model.data_loader import load_merged_data


bin_data = load_merged_data().set_index("BinID")

map_data = (
    bin_data.loc[bin_data["HasCoords"], ["Lat", "Lng", "Fill", "Temp", "Battery"]]
    .reset_index()
)

URGENT_FILL = 85
URGENT_TEMP = 60
LOW_BATTERY = 20

urgent_alerts = bin_data[
    (bin_data["Fill"] >= URGENT_FILL) |
    (bin_data["Temp"] >= URGENT_TEMP) |
    (bin_data["Battery"] <= LOW_BATTERY)
].copy()

urgent_bin_columns = urgent_alerts[["Fill", "Temp", "Battery"]]