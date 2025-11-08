"""
test_weather_api.py
Quick smoke test for Open-Meteo integration via repository.py

Usage:
  # All sensors from static_bin_data
  python test_weather_api.py

  # Only the specified sensors
  python test_weather_api.py --sensors R718X-001 R718X-002

  # Show raw JSON per unique coordinate (debug)
  python test_weather_api.py --debug
"""
from __future__ import annotations

import sys
import argparse
from datetime import timezone
import pandas as pd

import repository as repo


def _print_header(text: str) -> None:
    print("\n" + "=" * 80)
    print(text)
    print("=" * 80)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Test weather for bins via repository.py")
    parser.add_argument(
        "--sensors",
        nargs="*",
        help="Optional sensor_id list to test (defaults to all in static_bin_data)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Also print one raw fetch per unique coordinate for sanity",
    )
    args = parser.parse_args(argv)

    # 1) Load static bins to know which sensors & coords exist
    try:
        static_df = repo.fetch_static_bins_df()
    except Exception as e:
        print(f"ERROR: fetch_static_bins_df() failed: {e}")
        return 2

    if static_df is None or static_df.empty:
        print("No static bins found. Add entries to static_bin_data first.")
        return 1

    required_cols = {"sensor_id", "lat", "lng"}
    if not required_cols.issubset(set(static_df.columns)):
        print(f"static_bin_data is missing columns: {sorted(required_cols - set(static_df.columns))}")
        return 2

    # Optional filter
    if args.sensors:
        static_df = static_df[static_df["sensor_id"].isin(args.sensors)]
        if static_df.empty:
            print("No matching sensors found in static_bin_data for:", args.sensors)
            return 1

    sensor_ids = static_df["sensor_id"].unique().tolist()

    _print_header("Fetching current weather per sensor (from Open-Meteo via repository.py)")

    # 2) Call your repo weather helper
    try:
        wx = repo.fetch_weather_now_for_sensors(sensor_ids=sensor_ids)
    except Exception as e:
        print(f"ERROR: fetch_weather_now_for_sensors() failed: {e}")
        return 2

    if wx is None or wx.empty:
        print("Weather fetch returned no rows (check internet access / API).")
        return 1

    # 3) Make a nice table (also show coords for context)
    view = (
        static_df[["sensor_id", "lat", "lng"]]
        .merge(wx, on="sensor_id", how="left")
        .copy()
    )

    # Ensure types are nice for display
    if "temperature_c" in view.columns:
        view["temperature_c"] = pd.to_numeric(view["temperature_c"], errors="coerce").round(1)

    # Add a local-time helper column (Melbourne) for convenience; wx_time_utc may be NaT
    if "wx_time_utc" in view.columns:
        try:
            view["wx_time_local"] = pd.to_datetime(view["wx_time_utc"], utc=True).dt.tz_convert("Australia/Melbourne")
        except Exception:
            # If tz conversion fails for any reason, just keep UTC
            view["wx_time_local"] = view["wx_time_utc"]

    # Order columns for readability
    cols = [c for c in ["sensor_id", "lat", "lng", "temperature_c", "wx_time_utc", "wx_time_local"] if c in view.columns]
    view = view[cols].sort_values("sensor_id")

    # Print as a compact table
    with pd.option_context("display.max_rows", 200, "display.width", 120):
        print(view.to_string(index=False))

    # 4) Optional: show one raw call per unique coordinate (proves caching/requests path)
    if args.debug:
        _print_header("Raw per-coordinate 'now' fetch (first 5 unique coords)")
        uniq = view[["lat", "lng"]].dropna().drop_duplicates().head(5)
        for _, row in uniq.iterrows():
            lat, lng = float(row["lat"]), float(row["lng"])
            try:
                raw = repo.fetch_weather_now_by_coords(lat, lng)
                print(f"({lat:.5f}, {lng:.5f}) -> {raw}")
            except Exception as e:
                print(f"({lat:.5f}, {lng:.5f}) -> ERROR: {e}")

    print("\nOK: Weather API test completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
