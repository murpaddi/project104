from pathlib import Path
import pandas as pd

df = pd.DataFrame({
    'lat': [-37.7932],
    'lng': [144.8990]
})

#Replace with actual data source
CSV_PATH = Path(__file__).with_name("Random_Bin_Data.csv")
if not CSV_PATH.exists():
    raise FileNotFoundError(f"Could not find {CSV_PATH}")

bin_data = pd.read_csv(CSV_PATH, index_col='BinID')

#Replace with actual data source
urgent_alerts = bin_data[(bin_data['Fill'] >= 85) | (bin_data['Temp'] >= 60) | (bin_data['Battery'] <= 20)]
urgent_bin_columns = urgent_alerts[['Fill', 'Temp', 'Battery']]

map_coords = bin_data[['Lat', 'Lng']].copy()
map_coords["BinID"] = map_coords.index