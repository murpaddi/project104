from pathlib import Path
from Model.data_loader import load_live_with_coords

df = load_live_with_coords()

BASE_DIR = Path(__file__).resolve().parent / "Data"
TEST_OUTPUT = BASE_DIR / "test_output.csv"
df.to_csv(TEST_OUTPUT, index=True)
          
print(df.head())
print(df.columns.tolist())