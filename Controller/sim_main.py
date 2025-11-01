import time
import random
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

from Model.NetvoxR718x import NetvoxR718x
from Model import repository as repo

INTERVAL_MINUTES = 15
WRITE_INTERVAL_SECONDS = 900 #Change for accelerated testing

BASE_DIR = Path(__file__).resolve().parent.parent / "Model"
DATA_DIR = BASE_DIR / "Data"
LOGS_DIR = DATA_DIR / "Logs"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)


MASTER_CSV = DATA_DIR / "master_sensor_data.csv"
COORDS_CSV = DATA_DIR / "coordinates.csv"

RESET_MASTER = True
if RESET_MASTER and MASTER_CSV.exists():
    MASTER_CSV.unlink()
if COORDS_CSV.exists():
    COORDS_CSV.unlink()

RESET_DB = False #Set to True to reset DB on each run

def main():
    if RESET_DB:
        repo.reset_all(preserve_static=True) #Change to True to keep static data
        print("Database reset completed.")
    sensors = []
    coords_rows = []
    for i in range (1, 6):
        sensor_id = f"R718X-{i:03d}"
        bin_id = f"BIN-{i:03d}"
        lat = random.uniform(-37.7923, -37.7942)
        lng = random.uniform(144.8988, 144.9002)
        sensors.append(NetvoxR718x(
            sensor_id=sensor_id,
            fill_level_percent=random.randint(1, 100), # tweak starting fill level for testing
            temperature_c=random.uniform(15.0, 25.0), # tweak starting temp for testing
            battery_v=3.6,
            fill_threshold=85,
            #lat=random.uniform(-37.8136, -37.7000),
            #lng=random.uniform(144.9631, 145.2000),
            enable_traffic = True,
            fill_sentivity = random.randint(1, 5)
        ))

        coords_rows.append({"bin_id": bin_id, "sensor_id": sensor_id, "lat": lat, "lng": lng})
    
    pd.DataFrame(coords_rows).to_csv(COORDS_CSV, index=False) #Remove eventually, keeping for testing/debugging
    print(f"Coordinates saved to {COORDS_CSV}")

    # repo.upsert_static_bins(pd.DataFrame(coords_rows)) #Inserts rows to supabase. Make defunct later.
    # print("Static bin coordinates upserted to Supabase.")
        
    WRITE_LOCAL_CSV = False #Set to False to disable local CSV logging

    csv_paths = {}
    for s in sensors:
        per_sensor_csv = LOGS_DIR / f"{s.sensor_id}_data_log.csv"
        if per_sensor_csv.exists():
            per_sensor_csv.unlink()
        csv_paths[s.sensor_id] = per_sensor_csv

    header_needed_per_sensor = {s.sensor_id: True for s in sensors}
    master_header_needed = not MASTER_CSV.exists()

    try:
        while True:
            rows_to_write = []
            for s in sensors:
                s.simulate_changes(dt_minutes=INTERVAL_MINUTES)
                s.update_temperature()
                s.attempt_empty_event()

                #Ensures timestamp is a datetime object in current UTC time.
                #Failsafe if class timestamp not updated correctly.
                s.timestamp = datetime.now(timezone.utc)

                row = s.to_dict()

                #Convert timestamp strings to datetime objects for DB write
                for tcol in ("timestamp", "last_emptied", "last_overflow"):
                    if row.get(tcol) is not None:
                        row[tcol] = pd.to_datetime(row[tcol], utc = True)

                rows_to_write.append(row)

                df_one = pd.DataFrame([row])

                if WRITE_LOCAL_CSV:
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
                
            try:
                repo.write_archive_rows(rows_to_write)
            except Exception as e:
                print(f"[DB WRITE ERROR] {e}")


            time.sleep(WRITE_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nStream stopped.")

if __name__ == "__main__":
    main()