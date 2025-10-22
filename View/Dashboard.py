import streamlit as st
from View import Utilities as util

def show_dashboard():
    util.remove_elements()

    #Title
    st.title("Maribyrnong Smart City Bins Dashboard - Project 104")
    st.divider()

    if util.refresh_button("Refresh now"):
        st.rerun()

    auto_enabled, auto_interval = util.auto_refresh_controls()
    
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
        col1, col2 = util.double_column()

        with col1:
            st.subheader("Bin Status Summary Table")
            summary_cols = [
                "timestamp", "Fill", "Temp", "Battery",
                "Last Emptied", "Overflow Count", "Last Overflow"
            ]

            util.render_table(df[summary_cols], height = 360)
            util.download_button_from_df(
                df.reset_index(),
                filename = "Bin_Status_Summary.csv",
                label = "Export Bin Status to CSV"
            )

        with col2:
            st.subheader("Urgent Alerts")
            urgent_cols = ["timestamp", "Alert"]

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
                util.render_table(urgent_df[urgent_cols], height = 360)
                util.download_button_from_df(
                    urgent_df,
                    filename='Urgent_Alerts.csv',
                    label="Export Urgent Alerts to CSV",
                    key = "export_urgents_btn"
            )
                
    util.maybe_autorefresh(auto_enabled, auto_interval)
