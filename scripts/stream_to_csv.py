import time
from pathlib import Path
import pandas as pd
from Model.NetvoxR718x import NetvoxR718x

INTERVAL_SECONDS = 5

def main():
    sensors = [
        NetvoxR718x(
            sensor_id="R718X-001",
            fill_level_percent=50, temperature_c=25.0, battery_v=3.18,
            fill_threshold=85, lat=-37.7932, lng=144.8990)
    ]

    csv_paths = {}
    for s in sensors:
        csv_path = Path(f"Model/{s.sensor_id}_data_log.csv")
        if csv_path.exists():
            csv_path.unlink()
        csv_paths[s.sensor_id] = csv_path

    header_needed = {s.sensor_id: True for s in sensors}

    print(f"Logging {len(sensors)} sensors every {INTERVAL_SECONDS} seconds.")

    try:
        while True:
            rows = []
            for s in sensors:
                s.simulate_changes()
                rows.append(s.to_dict())
            
                df = pd.DataFrame(rows)
                df.to_csv(csv_paths[s.sensor_id], mode='a', header=header_needed[s.sensor_id], index=False)
                header_needed[s.sensor_id] = False

                print(f"[{s.sensor_id}] {df.to_string(index=False, header = False)}")

            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStream stopped.")

if __name__ == "__main__":
    main()