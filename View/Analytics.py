# View/Analytics.py
import streamlit as st
import Utilities as util
import pandas as pd
import plotly.express as px
import os
import numpy as np

# temporary time-series data 
def _make_temp_timeseries(bin_ids) -> pd.DataFrame:
    """
    Returns a DataFrame with columns: ['ts','BinID','Temp','Fill'].
    """
    times = pd.date_range("2025-01-01 00:00", periods=12, freq="2H")  # 00:00..22:00
    rows = []

    for b in bin_ids:
        temp = np.linspace(6, 16, len(times)) + (hash(b) % 5) * 0.2
        fill = np.linspace(70, 90, len(times))
        if b == "BIN_001":
            mid = len(times) // 2
            fill[mid] = 10  

        for t, tval, fval in zip(times, temp, fill):
            rows.append({"ts": t, "BinID": b, "Temp": float(tval), "Fill": float(fval)})

    return pd.DataFrame(rows)


def show_analytics():
    # Load Data 
    csv_path = "Model/Random_Bin_Data.csv"
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    if (not os.path.exists(csv_path)) or os.stat(csv_path).st_size == 0:
        sample = pd.DataFrame({
            "BinID": [f"BIN_{i:03d}" for i in range(1, 11)],
            "Lat":   [-37.7932 + 0.0005*np.random.randn() for _ in range(10)],
            "Lng":   [144.8990 + 0.0005*np.random.randn() for _ in range(10)],
            "Fill":  [95, 82, 56, 8, 100, 44, np.nan, 71, 92, 12],
            "Temp":  [22.5, 23.1, 21.8, 22.0, 24.2, 22.7, 21.9, 23.5, 22.8, 22.3],
            "Battery": [3.9, 3.8, 3.7, 3.9, 3.8, 3.9, 3.6, 3.8, 3.9, 3.7],
        })
        sample.to_csv(csv_path, index=False)

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        st.error(f"Couldn't read {csv_path}: {e}")
        return

    # Derive Bin Status from Fill
    if "Fill" not in df.columns:
        st.error("Your CSV is missing a 'Fill' column. Columns found: " + ", ".join(map(str, df.columns)))
        return

    fill = pd.to_numeric(df["Fill"], errors="coerce")
    status = pd.cut(
        fill,
        bins=[-np.inf, 10, 69, 89, np.inf],
        labels=["Empty", "Half Full", "Nearly Full", "Full"]
    ).astype(object)
    status[fill.isna()] = "No Data"
    df["Bin Status"] = status

    # Counts
    order = ["Full", "Nearly Full", "Half Full", "Empty", "No Data"]
    counts = (
        df["Bin Status"]
        .value_counts()
        .reindex(order, fill_value=0)
        .rename_axis("Bin Status")
        .reset_index(name="Count")
    )

    
    

    left_top, right_top = util.double_column()

    with left_top:
        st.subheader("Bin Status")
        bar_fig = px.bar(counts, x="Bin Status", y="Count", labels={"Count": "# of bins"})
        bar_fig.update_layout(xaxis={"categoryorder": "array", "categoryarray": order})
        st.plotly_chart(bar_fig, use_container_width=True)

    with right_top:
        st.subheader("Bin Status — Total Bins")
        pie_fig = px.pie(
            counts,
            names="Bin Status",
            values="Count",
            hole=0.35,                
        )
        
        pie_fig.update_layout(legend=dict(traceorder="normal"))
        st.plotly_chart(pie_fig, use_container_width=True)

    #  line charts
    bin_ids = df["BinID"].astype(str).tolist() if "BinID" in df.columns else ["BIN_001", "BIN_002", "BIN_003"]
    ts = _make_temp_timeseries(bin_ids)

    chosen_bin = st.selectbox("Choose Bin", options=sorted(ts["BinID"].unique()), index=0)
    sel = ts[ts["BinID"] == chosen_bin].sort_values("ts")


    # Line charts 2
    c1, c2 = util.double_column()

    with c1:
        st.subheader("Temperature over Time")
        temp_fig = px.line(sel, x="ts", y="Temp", markers=True, labels={"ts": "", "Temp": "°C"})
        temp_fig.update_layout(yaxis_title="°C")
        st.plotly_chart(temp_fig, use_container_width=True)

    with c2:
        st.subheader("Fill")
        fill_fig = px.line(sel, x="ts", y="Fill", markers=True, labels={"ts": "", "Fill": "%"})
        fill_fig.update_layout(yaxis_title="%", yaxis_range=[0, 100])
        st.plotly_chart(fill_fig, use_container_width=True)
