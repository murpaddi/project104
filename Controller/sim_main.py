import time
import random
import os
import pandas as pd
from datetime import datetime, timezone
from typing import List

from Model.NetvoxR718x import NetvoxR718x
from Model import repository as repo

SIM_COUNT = int(os.environ.get("SIM_COUNT", "6"))
WRITE_INTERVAL_SECONDS = int(os.environ.get("WRITE_INTERVAL_SECONDS", "900"))
SKIP_STARTUP_EMIT = os.environ.get("SKIP_STARTUP_EMIT", "1") == "1"
MANAGE_STATIC = os.environ.get("MANAGE_STATIC", "0") == "1"
HEARTBEAT_SECS = int(os.environ.get("HEARTBEAT_SECS", "3600"))

LAT_MIN, LAT_MAX = -37.7942, -37.7923
LNG_MIN, LNG_MAX = 144.8988, 144.9002

# === HELPER ===

def _advance_sensor(s):
    for m in ("step", "tick", "advance", "simulate_step", "simulate", "update"):
        if hasattr(s, m):
            getattr(s, m)()
            return

# === MAIN ===

def main():
    print(f"Booting simulators: SIM_COUNT={SIM_COUNT}, WRITE_INTERVAL_SECONDS={WRITE_INTERVAL_SECONDS}, "
          f"MANAGE_STATIC={MANAGE_STATIC}, SKIP_STARTUP_EMIT={SKIP_STARTUP_EMIT}, HEARTBEAT_SECS={HEARTBEAT_SECS}")
    
    repo.ensure_archive_unique_index()

    last = repo.fetch_any_latest_snapshot_df()
    last_idx = last.set_index("sensor_id") if not last.empty else pd.DataFrame()

    sensors: List[NetvoxR718x] = []
    coords_rows = []

    for i in range (1, SIM_COUNT + 1):
        sensor_id = f"R718X-{i:03d}"
        bin_id = f"BIN-{i:03d}"

        #Defaults for first run
        start_fill = random.randint(5, 60)
        start_temp = random.uniform(16.0, 24.0)
        start_batt = 3.6
        fill_thresh = 85

        if not last_idx.empty and sensor_id in last_idx.index:
            row = last_idx.loc[sensor_id]
            #Continue from database values, not random values
            start_fill = float(row.get("fill_level_percent", start_fill))
            start_temp = float(row.get("temperature_c", start_temp))
            start_batt = float(row.get("battery_v", start_batt))
            fill_thresh = int(row.get("fill_threshold", fill_thresh))

        sensors.append(NetvoxR718x(
            sensor_id=sensor_id,
            fill_level_percent=start_fill,
            temperature_c=start_temp,
            battery_v=start_batt,
            fill_threshold=fill_thresh,
            enable_traffic = True,
            fill_sentivity = random.randint(1, 5)
        ))

        coords_rows.append({
            "bin_id": bin_id,
            "sensor_id": sensor_id,
            "lat": random.uniform(LAT_MIN, LAT_MAX),
            "lng": random.uniform(LNG_MIN, LNG_MAX)
        })

    if MANAGE_STATIC:
        df_coords = pd.DataFrame(coords_rows)
        repo.sync_static_bins(df_coords, delete_missing=True, update_existing=False)
        print(f"Static sync complete for {SIM_COUNT} bins.")
    else:
        print(f"Static sync skipped (MANAGE_STATIC=0).")

        # === MAIN LOOP ===
    first_cycle = True
    while True:
        rows_to_write = []
        for s in sensors:
            _advance_sensor(s)

            row = s.to_dict()

            row["timestamp"] = pd.Timestamp.utcnow()

            #Convert timestamp strings to datetime objects for DB write
            for tcol in ("timestamp", "last_emptied", "last_overflow"):
                if row.get(tcol) is not None:
                    row[tcol] = pd.to_datetime(row[tcol], utc = True)

            if SKIP_STARTUP_EMIT and first_cycle:
                continue

            rows_to_write.append(row)

        first_cycle = False

        if rows_to_write:
            repo.write_archive_rows(rows_to_write)

            print(f"[{pd.Timestamp.now().strftime('%H:%M:%S')}] "
                  f"Wrote {len(rows_to_write)} bin records to database")
        else:
            print(f"[{pd.Timestamp.now().strftime('%H:%M:%S')}] No rows written this cycle")

        time.sleep(WRITE_INTERVAL_SECONDS)            

if __name__ == "__main__":
    main()