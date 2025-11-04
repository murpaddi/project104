import streamlit as st
from View import Utilities as util
import pandas as pd

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
            fmt = "%d %b %Y, %#I:%M %p"
            for col in ("Timestamp", "Last Emptied", "Last Overflow"):
                if col in display_df.columns:
                    display_df[col] = pd.to_datetime(display_df[col]).dt.strftime(fmt)

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
                urgent_display = urgent_df.copy()
                if "Timestamp" in urgent_display.columns:
                    urgent_display["Timestamp"] = pd.to_datetime(urgent_display["Timestamp"]).dt.strftime("%d %b %Y, %#I:%M %p")
                util.render_table(urgent_display[urgent_cols], height = 360)
                util.download_button_from_df(
                    urgent_display,
                    filename='Urgent_Alerts.csv',
                    label="Export Urgent Alerts to CSV",
                    key = "export_urgents_btn"
            )
    
    st.divider()

#DOWNLOAD FILE DATA
    with st.container():
        st.header("Download Bin Data")
        st.selectbox(label="Select Bin", options=None)
        st.selectbox(label="Format", options=None) #List of different formats
        st.download_button(label="Download", data="Hi") #Replace with the filetype described in format
            
                
    util.maybe_autorefresh(auto_enabled, auto_interval)
