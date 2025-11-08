import time
import random
import os
import pandas as pd
from datetime import datetime, timezone
from typing import List

from Model.NetvoxR718x import NetvoxR718x
from Model import repository as repo

# === CONFIG ===
SIM_COUNT = int(os.environ.get("SIM_COUNT", "6"))
WRITE_INTERVAL_SECONDS = int(os.environ.get("WRITE_INTERVAL_SECONDS", "900"))
SKIP_STARTUP_EMIT = os.environ.get("SKIP_STARTUP_EMIT", "1") == "1"
MANAGE_STATIC = os.environ.get("MANAGE_STATIC", "0") == "1"
HEARTBEAT_SECS = int(os.environ.get("HEARTBEAT_SECS", "3600"))
REPORT_JITTER_SECONDS = int(os.environ.get("REPORT_JITTER_SECONDS", "0"))
MIN_SLEEP_SECONDS = int(os.environ.get("MIN_SLEEP_SECONDS", "1"))

LAT_MIN, LAT_MAX = -37.7942, -37.7923
LNG_MIN, LNG_MAX = 144.8988, 144.9002

USE_WEATHER_TEMP = os.environ.get("USE_WEATHER_TEMP", "1") == "1"
WEATHER_JITTER_C = float(os.environ.get("WEATHER_JITTER_C", "0.0"))

# === HELPER ===

def _advance_sensor(s, dt_minutes: int | None = None):
    if hasattr(s, "simulate_changes"):
        s.simulate_changes(dt_minutes=dt_minutes or 15)
        if hasattr(s, "attempt_empty_event"):
            try:
                s.attempt_empty_event(base_threshold=getattr(s, "fill_threshold", 85), empty_chance=0.005)
            except Exception:
                pass
        if hasattr(s, "update_temperature"):
            try:
                s.update_temperature()
            except Exception:
                pass
        return
    for m in ("step", "tick", "advance", "simulate_step", "simulate", "update"):
        if hasattr(s, m):
            getattr(s, m)()
            return

# === MAIN ===

def main():
    print(
        f"Booting simulators: SIM_COUNT={SIM_COUNT}, WRITE_INTERVAL_SECONDS={WRITE_INTERVAL_SECONDS}, "
        f"MANAGE_STATIC={MANAGE_STATIC}, SKIP_STARTUP_EMIT={SKIP_STARTUP_EMIT}, HEARTBEAT_SECS={HEARTBEAT_SECS}, "
        f"REPORT_JITTER_SECONDS={REPORT_JITTER_SECONDS}, MIN_SLEEP_SECONDS={MIN_SLEEP_SECONDS}, "
        f"USE_WEATHER_TEMP={USE_WEATHER_TEMP}, WEATHER_JITTER_C={WEATHER_JITTER_C}"
    )
    
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
            start_fill = int(row.get("fill_level_percent", start_fill))
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

    #=== STAGGERED SCHEDULER ===
    now_utc = pd.Timestamp.utcnow()
    next_due = {}

    for s in sensors:
        sid = s.sensor_id
        offset = random.randint(0, max(WRITE_INTERVAL_SECONDS - 1, 0))
        due = now_utc + pd.Timedelta(seconds=offset)
        if SKIP_STARTUP_EMIT:
            due = due + pd.Timedelta(seconds=WRITE_INTERVAL_SECONDS)
        next_due[sid] = due

    # === MAIN LOOP ===
    while True:
        now = pd.Timestamp.utcnow()
        rows_to_write = []

        for s in sensors:
            sid = s.sensor_id
            due = next_due[sid]

            if now >= due:
                last_ts = getattr(s, "timestamp", None)
                if last_ts is None:
                    dt_min = max(1, WRITE_INTERVAL_SECONDS // 60)
                else:
                    last_ts = pd.to_datetime(last_ts, utc=True)
                    dt_min = max(1, int((pd.Timestamp.utcnow() - last_ts).total_seconds() // 60))

                #Diagnostics
                before_fill = int(getattr(s, "fill_level_percent", 0.0))
                before_ts = getattr(s, "timestamp", None)

                _advance_sensor(s, dt_minutes=dt_min)

                #Diagnositics
                after_fill = int(getattr(s, "fill_level_percent", before_fill))
                after_ts = getattr(s, "timestamp", None)

                print(f"[{pd.Timestamp.now().strftime('%H:%M:%S')}] {sid}: dt={dt_min}m "
                    f"fill {before_fill:.1f} -> {after_fill:.1f} "
                    f"(ts {before_ts} -> {after_ts})")
                
                if abs(after_fill - before_fill) < 1e-6:
                # fallback deltas; tune via env if you wish
                    FMIN = float(os.environ.get("FALLBACK_FILL_MIN_DELTA", "0.3"))
                    FMAX = float(os.environ.get("FALLBACK_FILL_MAX_DELTA", "1.2"))
                    nudge = random.uniform(FMIN, FMAX)
                    after_fill = min(100.0, before_fill + nudge)
                    setattr(s, "fill_level_percent", after_fill)
                    print(f"[{pd.Timestamp.now().strftime('%H:%M:%S')}] {sid}: fallback nudge +{nudge:.2f} -> {after_fill:.1f}")

                row = s.to_dict()

                row["timestamp"] = pd.Timestamp.utcnow()

                #Convert timestamp strings to datetime objects for DB write
                for tcol in ("timestamp", "last_emptied", "last_overflow"):
                    if row.get(tcol) is not None:
                        row[tcol] = pd.to_datetime(row[tcol], utc = True)

                rows_to_write.append(row)

                jitter = 0
                if REPORT_JITTER_SECONDS > 0:
                    max_jitter = min(REPORT_JITTER_SECONDS, max(WRITE_INTERVAL_SECONDS -1, 0))
                    jitter = random.randint(-max_jitter, max_jitter)
                next_due[sid] = due + pd.Timedelta(seconds=max(1, WRITE_INTERVAL_SECONDS + jitter))


        if rows_to_write and USE_WEATHER_TEMP:
            try:
                sensor_ids = list({r["sensor_id"] for r in rows_to_write})
                wx = repo.fetch_weather_now_for_sensors(sensor_ids=sensor_ids)
                temp_map = {}
                if wx is not None and not wx.empty:
                    for _, r in wx.iterrows():
                        t = r.get("temperature_c")
                        if pd.notna(t):
                            temp_map[str(r["sensor_id"])] = float(t)
                for r in rows_to_write:
                    sid = r["sensor_id"]
                    if sid in temp_map:
                        t = temp_map[sid]
                        if WEATHER_JITTER_C > 0:
                            t += random.uniform(-WEATHER_JITTER_C, WEATHER_JITTER_C)
                        r["temperature_c"] = round(float(t), 1)
            except Exception as e:
                print("WARNING: weather fetch failed; using simulated temperatures", repr(e))


        if rows_to_write:
            try:
                repo.write_archive_rows(rows_to_write)
                ids = ", ".join(r["sensor_id"] for r in rows_to_write)
                print(f"[{pd.Timestamp.now().strftime('%H:%M:%S')}] "
                    f"Wrote {len(rows_to_write)} records -> {ids}")
            except Exception as e:
                print("ERROR writing archive rows:", repr(e))
        else:
            print(f"[{pd.Timestamp.now().strftime('%H:%M:%S')}] No rows written this cycle")

        soonest = min(next_due.values()) if next_due else (now + pd.Timedelta(seconds=WRITE_INTERVAL_SECONDS))
        wait_s = int((soonest - pd.Timestamp.utcnow()).total_seconds())
        if wait_s < MIN_SLEEP_SECONDS:
            wait_s = MIN_SLEEP_SECONDS
        time.sleep(wait_s)         

if __name__ == "__main__":
    main()