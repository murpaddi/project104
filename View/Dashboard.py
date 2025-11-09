import streamlit as st
from View import Utilities as util
import pandas as pd
from Model import repository as repo
from datetime import datetime, time as dtime


def show_dashboard():
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

    with st.container():
        util.render_map_section(map_data)

    st.divider()

    #Data Overview
    with st.container():
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

            fmt = "%d %b %Y, %-I:%M %p"
            try:
                for col in ("Timestamp", "Last Emptied", "Last Overflow"):
                    if col in display_df.columns:
                        display_df[col] = pd.to_datetime(display_df[col], errors = "coerce").dt.strftime(fmt)
            except Exception:
                fmt_win = "%d %b %Y, %#I:%M %p"
                for col in ("Timestamp", "Last Emptied", "Last Overflow"):
                    if col in display_df.columns:
                        display_df[col] = pd.to_datetime(display_df[col], errors = "coerce").dt.strftime(fmt_win)

            util.render_table(display_df[summary_cols], height = 360)
            util.download_button_from_df(
                df.reset_index(),
                filename = "Bin_Status_Summary.csv",
                label = "Export Bin Status to CSV"
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
    
    if "download_divider" not in st.session_state:
        st.session_state["download_divider"] = st.empty()

        # Replace divider each run rather than stacking multiples
        st.session_state["download_divider"].empty()
        st.session_state["download_divider"].divider()

#DOWNLOAD FILE DATA

    if "download_root" not in st.session_state or st.session_state["download_root"] is None:
        st.session_state["download_root"] = st.empty()
    
    st.session_state["download_root"].empty()

    with st.session_state["download_root"].container():
        st.header("Download Bin Data")

        def _bin_ids():
            df = repo.fetch_archive_df(columns="sensor_id")
            return sorted(df["sensor_id"].unique()) if not df.empty else []
        
        bin_options = list(_bin_ids())
        ALL = "All bins"
        SENTINEL = "--No bins available--"
        choices = ([ALL] + bin_options) if bin_options else [SENTINEL]
        selected_bin = st.selectbox("Select Bin", choices, key="dl_bin")

        col1,col2,col3 = util.triple_column()
        with col1:
            since_date = st.date_input("Start date")
            since_time = st.time_input("Start time", value=dtime(0,0))
        with col2:
            until_date = st.date_input("End date")
            until_time = st.time_input("End time", value=dtime(23, 59))
        with col3:
            limit = st.number_input("Row limit (0 = no limit)", min_value=0, value=0, step=1000)
        
        since = datetime.combine(since_date, since_time)
        until = datetime.combine(until_date, until_time)

        fmt = st.selectbox(
            "Format", 
            ["CSV", "JSON", "Parquet", "HTML", "XML", "Feather"],
            key = "dl_fmt"
        )

        if selected_bin != SENTINEL:
            device_id = None if selected_bin == ALL else selected_bin
            df = util.get_archive_with_coords_df(
                device_id,
                since=since,
                until=until,
                limit=None if limit == 0 else int(limit)
            )
            if isinstance(df, pd.DataFrame) and not df.empty:
                data, mime, ext = util.prepare_download(df, fmt)
                base = "all_bins" if device_id is None else device_id
                st.download_button(
                    label=f"Download {fmt}",
                    data=data,
                    file_name=f"{base}_data.{ext}",
                    mime=mime,
                    key="dl_btn")
            else:
                st.info("No data found for the selected bin and time window")
            
                
    util.maybe_autorefresh(auto_enabled, auto_interval)
