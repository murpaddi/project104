import pandas as pd

df = pd.DataFrame({
    'lat': [-37.7932],
    'lng': [144.8990]
})

bin_data = pd.read_csv("Random_Bin_Data.csv", index_col=0)