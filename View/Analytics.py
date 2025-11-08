import streamlit as st
import pandas as pd
from datetime import timedelta
from Model.data_loader import load_live_with_coords
import plotly.express as px
from View import Utilities as util



# ---- Main Page ----
def show_analytics():
    util.remove_elements()
    key_prefix = "ana_"

    st.title("Smart Bin Analytics")

    enabled, interval = util.auto_refresh_controls(key_prefix=key_prefix)
    #Load latest master data
    df = load_live_with_coords()

    if "Timestamp" in df.columns:
        df["Timestamp"] = pd.to_datetime(df["Timestamp"]).dt.strftime("%d %b %Y, %-I:%M %p")
    if df.empty:
        st.warning("No live data available. Please start the simulator.")
        return
    
    st.subheader("Overall Summary")

    col1, col2 = util.double_column()

    with col1:
        st.metric("Average Fill", f"{df['Fill'].mean():.1f}%")
        st.metric("Average Temperature", f"{df['Temperature'].mean():.1f}°C")

        st.subheader("Fill Level Distribution")
        fig_hist = px.histogram(
            df, x ="Fill", nbins=10,
            color_discrete_sequence=["#0083B8"],
            title = "Distribution of Bin Fill Levels (%)"
        )
        fig_hist.update_layout(xaxis_title="Fill Level (%)", yaxis_title="Count of Bins", bargap=0.15)
        st.plotly_chart(fig_hist, width="stretch")




    with col2:
        st.metric("Average Battery", f"{df['Battery'].mean():.2f}V")
        st.metric("Bins Reporting", len(df))


        #Pie Chart
        st.subheader("Fill Level Distribution")
        fill_bins = pd.cut(
            df["Fill"],
            bins = [0,25,50,75,100],
            labels=["0-25%", "26-50%", "51-75%", "76-100%"]
        )
        fill_counts = fill_bins.value_counts().sort_index()
        fig = px.pie(
            values=fill_counts.values,
            names=fill_counts.index,
            color_discrete_sequence=px.colors.qualitative.Set2,
            hole=0.15,
            title="Proportion of Bins by Fill Range"
        )
        st.plotly_chart(fig, width="stretch")
    
    st.divider()

    st.header("Individual Bin Analysis")

    col1, col2 = util.double_column()
    bin_ids = df.index.astype(str).tolist()
    with col1:
        selected_bin = st.selectbox("Select a Bin", bin_ids, key=f"{key_prefix}bin_select")
    
    with col2:
        window = st.selectbox(
            "Time Window",
            ["15 Minutes (testing)", "30 Minutes (testing)", "Hourly", "6 Hours", "12 Hours", "24 Hours", "48 Hours", "7 Days"],
            index = 3,
            key=f"{key_prefix}window_select"
            )

        WINDOWS = {
            "15 Minutes (testing)": timedelta(minutes=15),
            "30 Minutes (testing)": timedelta(minutes=30),
            "Hourly": timedelta(hours=1),
            "6 Hours": timedelta(hours=6),
            "12 Hours": timedelta(hours=12),
            "24 Hours": timedelta(hours=24),
            "48 Hours": timedelta(hours=48),
            "7 Days": timedelta(days=7)
            }

    if selected_bin:
            device_id = str(df.loc[selected_bin, "DeviceID"])
            log_df = util.load_bin_log(device_id)

            if log_df.empty:
                 st.info(f"No log data found for device {device_id}.")
            else:

                if "Timestamp" in log_df.columns and not log_df["Timestamp"].empty:
                    now = log_df["Timestamp"].max()
                    delta = WINDOWS[window]
                    start_time = now - delta
                    log_window = log_df[log_df["Timestamp"].between(start_time, now)]
                else:
                    log_window = log_df.copy()
                    start_time = None
                    now = None

                c1, c2 = util.double_column()

                with c1:
                    st.subheader("Fill Level Over Time")
                    fig_fill = px.line(
                        log_window, x = "Timestamp", y = "Fill",
                        title = f"Fill Level Trend - {selected_bin}",
                        markers = True
                    )
                    kw = dict(yaxis_title="Fill Level (%)", yaxis_range=[0, 100])
                    if start_time is not None:
                        kw["xaxis_range"] = [start_time, now]
                    fig_fill.update_layout(**kw)
                    st.plotly_chart(fig_fill, width="stretch")              
            
                with c2:
                    st.subheader("Temperature Over Time")
                    fig_temp = px.line(
                        log_window, x = "Timestamp", y = "Temperature",
                        title = "Temperature Flux", markers = True
                    )
                    kw = dict(yaxis_title="Temperature (°C)", yaxis_range=[-5, 60], xaxis_title=None)
                    if start_time is not None:
                        kw["xaxis_range"] = [start_time, now]
                    fig_temp.update_layout(**kw)
                    st.plotly_chart(fig_temp, width="stretch")
                
                if log_window.empty:
                    st.info(f"No data in the selected window ({window})")


                
    util.maybe_autorefresh(enabled, interval)