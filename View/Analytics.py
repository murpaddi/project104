import streamlit as st
import Utilities as util
import pandas as pd
import plotly.express as px
import os
import numpy as np
from io import BytesIO


# ---- Helper function to simulate time-series data ----
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


# ---- Helper: Convert DataFrame to Excel ----
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()


# ---- Main Page ----
def show_analytics():
    util.remove_elements()

    # ---- Theme-Aware CSS Styling ----
    st.markdown("""
        <style>
            :root {
                --bg-color: var(--background-color);
                --text-color: var(--text-color);
                --secondary-bg-color: var(--secondary-background-color);
            }

            /* Section title boxes */
            .section-box {
                background: rgba(255, 255, 255, 0.03);
                border: 1px solid rgba(255, 255, 255, 0.1);
                border-left: 4px solid #00b4d8;
                border-radius: 10px;
                padding: 0.8rem 1.2rem;
                margin: 1.5rem 0 0.8rem 0;
                box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
            }
            .section-box h2 {
                margin: 0;
                font-size: 1.4rem;
                font-weight: 600;
                color: var(--text-color);
            }

            /* Export button styling */
            div.stDownloadButton > button {
                background-color: #00b4d8;
                color: white;
                border-radius: 8px;
                padding: 0.4rem 1rem;
                border: none;
                font-weight: 500;
                transition: background-color 0.2s ease-in-out;
            }
            div.stDownloadButton > button:hover {
                background-color: #0090b8;
            }
        </style>
    """, unsafe_allow_html=True)

    # ---- Load and Prepare Data ----
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

    if "Fill" not in df.columns:
        st.error("Your CSV is missing a 'Fill' column. Columns found: " + ", ".join(map(str, df.columns)))
        return

    # ---- Derive Bin Status ----
    fill = pd.to_numeric(df["Fill"], errors="coerce")
    status = pd.cut(
        fill,
        bins=[-np.inf, 10, 69, 89, np.inf],
        labels=["Empty", "Half Full", "Nearly Full", "Full"]
    ).astype(object)
    status[fill.isna()] = "No Data"
    df["Bin Status"] = status

    # ---- Count bins by status ----
    order = ["Full", "Nearly Full", "Half Full", "Empty", "No Data"]
    counts = (
        df["Bin Status"]
        .value_counts()
        .reindex(order, fill_value=0)
        .rename_axis("Bin Status")
        .reset_index(name="Count")
    )

    # ---- Section 1: Bin Status Charts ----
    st.markdown("<div class='section-box'><h2>Bin Status Overview</h2></div>", unsafe_allow_html=True)

    left_top, right_top = util.double_column()
    with left_top:
        st.subheader("Bin Status (Bar Chart)")
        bar_fig = px.bar(counts, x="Bin Status", y="Count", labels={"Count": "# of bins"})
        bar_fig.update_layout(xaxis={"categoryorder": "array", "categoryarray": order})
        st.plotly_chart(bar_fig, use_container_width=True)

    with right_top:
        st.subheader("Bin Status Distribution")
        pie_fig = px.pie(
            counts,
            names="Bin Status",
            values="Count",
            hole=0.35,
        )
        pie_fig.update_layout(legend=dict(traceorder="normal"))
        st.plotly_chart(pie_fig, use_container_width=True)

    # ---- Export Bin Summary ----
    st.download_button(
        label="⬇️ Export Bin Summary to Excel",
        data=to_excel(counts),
        file_name='Bin_Status_Summary.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

    # ---- Section 2: Time-Series Analysis ----
    st.markdown("<div class='section-box'><h2>Time-Series Analytics</h2></div>", unsafe_allow_html=True)

    bin_ids = df["BinID"].astype(str).tolist() if "BinID" in df.columns else ["BIN_001", "BIN_002", "BIN_003"]
    ts = _make_temp_timeseries(bin_ids)

    chosen_bin = st.selectbox("Select a Bin", options=sorted(ts["BinID"].unique()), index=0)
    sel = ts[ts["BinID"] == chosen_bin].sort_values("ts")

    # ---- Line Charts ----
    c1, c2 = util.double_column()

    with c1:
        st.subheader("Temperature Over Time")
        temp_fig = px.line(sel, x="ts", y="Temp", markers=True, labels={"ts": "", "Temp": "°C"})
        temp_fig.update_layout(yaxis_title="°C")
        st.plotly_chart(temp_fig, use_container_width=True)

    with c2:
        st.subheader("Fill Level Over Time")
        fill_fig = px.line(sel, x="ts", y="Fill", markers=True, labels={"ts": "", "Fill": "%"})
        fill_fig.update_layout(yaxis_title="%", yaxis_range=[0, 100])
        st.plotly_chart(fill_fig, use_container_width=True)

    # ---- Export the Original Data ----
    st.markdown("<div class='section-box'><h2>Export Full Dataset</h2></div>", unsafe_allow_html=True)
    st.download_button(
        label="⬇️ Export Bin Data to Excel",
        data=to_excel(df),
        file_name='All_Bin_Data.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
