import streamlit as st
from View import Utilities as util
import pandas as pd
from Model import repository as repo
from datetime import datetime, time as dtime, timedelta
import sys
from zoneinfo import ZoneInfo


def show_dashboard():
    st.session_state["_dl_rendered_this_rerun"] = False
    util.remove_elements()
    key_prefix = "dash_"

    #Title
    st.title("Maribyrnong Smart City Bins Dashboard - Project 104")
    st.divider()

    if util.refresh_button("Refresh now", key=f"{key_prefix}refresh_btn"):
        st.rerun()

    auto_enabled, auto_interval = util.auto_refresh_controls(key_prefix=key_prefix)
    
    #Load latest data
    df = util.get_latest_df(show_errors=True)

    if df.empty:
        util.maybe_autorefresh(auto_enabled, auto_interval)
        return
   
    #Prepare map and alerts
    map_data = util.prep_map_data(df)
    urgent_df = util.filter_urgent(df)

    with st.container(key=f"{key_prefix}map_container"):
        util.render_map_section(map_data)

    st.divider()

    #Data Overview
    with st.container(key=f"{key_prefix}overview_container"):
        st.header("Bin Data Overview")
        col1, col2 = util.two_to_one()

        with col1:
            st.subheader("Bin Status Summary Table")
            summary_cols = [
                "Timestamp", "Fill", "Temperature", "Battery",
                "Last Emptied", "Overflow #", "Last Overflow"
            ]

            display_df = df.copy()
            display_df = util.ensure_columns(display_df, summary_cols)

            fmt_linux = "%d %b %Y, %-I:%M %p"
            fmt_win = "%d %b %Y, %#I:%M %p"

            fmt = fmt_win if sys.platform.startswith("win") else fmt_linux
            for col in ("Timestamp", "Last Emptied", "Last Overflow"):
                if col in display_df.columns:
                    display_df[col] = pd.to_datetime(display_df[col], errors = "coerce").dt.strftime(fmt)

            util.render_table(display_df[summary_cols], height = 360)
            util.download_button_from_df(
                df.reset_index(),
                filename = "Bin_Status_Summary.csv",
                label = "Export Bin Status to CSV",
                key=f"{key_prefix}export_summary_btn"
            )

        with col2:
            st.subheader("Urgent Alerts")
            urgent_cols = ["Timestamp", "Alert"]

            if urgent_df.empty:
                st.success("No urgent alerts at this time.")
                util.download_button_from_df(
                    urgent_df,
                    filename = "Urgent_Alerts.csv",
                    label="Export Urgent Alerts to CSV",
                    key="export_urgent_btn",
                )
            else:
                urgent_df = urgent_df.set_index("BinID", drop=True)
                urgent_display = util.ensure_columns(urgent_df.copy(), urgent_cols)
                if "Timestamp" in urgent_display.columns:
                    try:
                        urgent_display["Timestamp"] = pd.to_datetime(urgent_display["Timestamp"]).dt.strftime("%d %b %Y, %-I:%M %p")
                    except Exception:
                        urgent_display["Timestamp"] = pd.to_datetime(urgent_display["Timestamp"], errors="coerce").dt.strftime("%d %b %Y, %#I:%M %p")

                util.render_table(urgent_display[urgent_cols], height = 360)
                util.download_button_from_df(
                    urgent_display,
                    filename='Urgent_Alerts.csv',
                    label="Export Urgent Alerts to CSV",
                    key = "export_urgents_btn"
            )
    
    st.divider()

#DOWNLOAD FILE DATA
    if not st.session_state.get("_dl_rendered_this_rerun", False):
        st.session_state["_dl_rendered_this_rerun"] = True  # mark as rendered
        dl_container = st.container()
        with dl_container:
            st.header("Export Bin Data")

            @st.cache_data(show_spinner=False, ttl=300)
            def _bin_ids():
                df = repo.fetch_archive_df(columns="sensor_id")
                if df is None or df.empty or "sensor_id" not in df.columns:
                    return[]
                return sorted(pd.Series(df["sensor_id"]).dropna().astype(str).unique().tolist())
            
            bin_options = _bin_ids()
            ALL = "All bins"
            SENTINEL = "--No bins available--"
            choices = ([ALL] + bin_options) if bin_options else [SENTINEL]
            selected_bin = st.selectbox("Select Bin", choices, key="dl_bin")

            tz = ZoneInfo("Australia/Melbourne")
            today = datetime.now(tz).date()
            default_start = today - timedelta(days=7)

            col1,col2,col3 = util.triple_column()
            with col1:
                since_date = st.date_input("Start date", value=default_start, key="dl_since_date")
                since_time = st.time_input("Start time", value=dtime(0,0), key="dl_since_time")
            with col2:
                until_date = st.date_input("End date", value=today, key="dl_until_date")
                until_time = st.time_input("End time", value=dtime(23, 59), key="dl_until_time")
            with col3:
                limit = st.number_input("Row limit (0 = no limit)", min_value=0, value=0, step=1000, key="dl_limit")

            fmt = st.selectbox(
                "Format", 
                ["CSV", "JSON", "Parquet", "HTML", "XML", "Feather"],
                key = "dl_fmt"
            )

            if selected_bin == SENTINEL:
                st.info("No bins are available yet.")
            else:
                ss = st.session_state
                ss.setdefault("dl_params", None)
                ss.setdefault("dl_payload", None)

                since = datetime.combine(since_date, since_time, tzinfo=tz)
                until = datetime.combine(until_date, until_time, tzinfo=tz)
                if since > until:
                    st.error("Start must be before end")
                else:
                    params = (selected_bin, since.isoformat(), until.isoformat(), int(limit), fmt)

                    def _prepare():
                        device_id = None if selected_bin == ALL else selected_bin
                        df = util.get_archive_with_coords_df(
                            device_id,
                            since=since,
                            until=until,
                            limit=None if limit == 0 else int(limit)
                        )
                        if not isinstance(df, pd.DataFrame) or df.empty:
                            st.info("No data found for the selected bin and time window")
                            ss.dl_payload = None
                            return
                        data, mime, ext = util.prepare_download(df, fmt)
                        base = "all_bins" if device_id is None else device_id
                        ss.dl_payload = (data, mime, ext, base, fmt)
                        ss.dl_params = params
                    
                    if ss.dl_params != params:
                        _prepare()

                    if ss.dl_payload:
                        data, mime, ext, base, fmt_now = ss.dl_payload
                        st.download_button(
                            label=f"Export {fmt_now}",
                            data=data,
                            file_name=f"{base}_data.{ext}",
                            mime=mime,
                            key="dl_btn"
                        )
            
                
    util.maybe_autorefresh(auto_enabled, auto_interval)
