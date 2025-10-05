import time
from pathlib import Path
import pandas as pd
from Model.NetvoxR718x import NetvoxR718x

INTERVAL_SECONDS = 5

BASE_DIR = Path(__file__).resolve().parent.parent / "Model"
DATA_DIR = BASE_DIR / "Data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MASTER_CSV = DATA_DIR / "master_sensor_data.csv"
RESET_MASTER = False

def main():
    sensors = [
        NetvoxR718x(
            sensor_id="R718X-001",
            fill_level_percent=50, temperature_c=25.0, battery_v=3.18,
            fill_threshold=85, lat=-37.7932, lng=144.8990)

            #more sensors can be added here
    ]

    csv_paths = {}
    for s in sensors:
        per_sensor_csv = DATA_DIR / f"{s.sensor_id}_data_log.csv"
        if per_sensor_csv.exists():
            per_sensor_csv.unlink()
        csv_paths[s.sensor_id] = per_sensor_csv

    header_needed_per_sensor = {s.sensor_id: True for s in sensors}
    master_header_needed = not MASTER_CSV.exists()


    print(f"Logging {len(sensors)} sensors every {INTERVAL_SECONDS} seconds.")

    try:
        while True:
            for s in sensors:
                s.simulate_changes()

                #Build a single-row DataFrame
                row = s.to_dict()
                df_one = pd.DataFrame([row])

                #Append to sensor specific CSV
                df_one.to_csv(
                    csv_paths[s.sensor_id], 
                    mode='a', 
                    header=header_needed_per_sensor[s.sensor_id], 
                    index=False
                )
                header_needed_per_sensor[s.sensor_id] = False

                #Append to master CSV
                df_one.to_csv(
                    MASTER_CSV, 
                    mode='a', 
                    header=master_header_needed, 
                    index=False
                )
                master_header_needed = False

                # Print to console
                print(f"[{s.sensor_id}] {df_one.to_string(index=False, header = False)}")

            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStream stopped.")

if __name__ == "__main__":
    main()