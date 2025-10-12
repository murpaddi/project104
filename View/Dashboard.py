import streamlit as st
import pydeck as pdk
import pandas as pd
import Utilities as util
from Model import testing_data as td
from io import BytesIO

def show_dashboard():
    util.remove_elements()

    # ---- Adaptive Styling (Dark/Light Mode) ----
    st.markdown("""
        <style>
            /* Use Streamlit's theme-aware CSS variables */
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

            /* Data table styling */
            .dataframe {
                border-collapse: collapse;
                width: 100%;
                border-radius: 10px !important;
                overflow: hidden;
                box-shadow: 0 0 10px rgba(0, 0, 0, 0.15);
            }
            .dataframe th {
                background-color: var(--secondary-bg-color) !important;
                color: var(--text-color) !important;
                font-weight: 600 !important;
                padding: 0.6rem !important;
                text-align: left !important;
                border-bottom: 1px solid rgba(255, 255, 255, 0.1) !important;
            }
            .dataframe td {
                background-color: var(--bg-color) !important;
                color: var(--text-color) !important;
                padding: 0.55rem !important;
                border-bottom: 1px solid rgba(255, 255, 255, 0.05) !important;
            }
            .dataframe tr:hover td {
                background-color: rgba(0, 180, 216, 0.08) !important;
            }

            /* Entire urgent rows in red */
            .urgent-row td {
                color: #ff4d4d !important;
                font-weight: 600 !important;
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

    # ---- Helper: Convert DataFrame to Excel ----
    def to_excel(df):
        output = BytesIO()
        # ✅ Uses openpyxl (no install required)
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
        return output.getvalue()

    # ---- Page Title ----
    with st.container():
        st.title("Maribyrnong Smart City Bins Dashboard - Project 104")
        st.divider()
    
    # ---- Real-Time Bin Monitoring Map ----
    with st.container():
        st.markdown("<div class='section-box'><h2>Real-Time Bin Monitoring Map</h2></div>", unsafe_allow_html=True)
        deck_obj = util.load_map(td.map_data)
        st.pydeck_chart(deck_obj)

    # ---- Bin Data Overview ----
    with st.container():
        st.markdown("<div class='section-box'><h2>Bin Data Overview</h2></div>", unsafe_allow_html=True)

        col1, col2 = util.double_column()
        with col1:
            st.markdown("<div class='section-box'><h2>Bin Status Summary Table</h2></div>", unsafe_allow_html=True)
            
            # Styled Bin Summary Table
            st.markdown(td.bin_data.style
                .set_table_attributes('class="dataframe"')
                .to_html(), unsafe_allow_html=True)

            # Export button for Bin Status Summary
            bin_excel = to_excel(td.bin_data)
            st.download_button(
                label="⬇️ Export Bin Status to Excel",
                data=bin_excel,
                file_name='Bin_Status_Summary.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

        with col2:
            st.markdown("<div class='section-box'><h2>Urgent Alerts</h2></div>", unsafe_allow_html=True)

            # Highlight urgent rows in red
            def highlight_urgent_rows(row):
                return ['color: #ff4d4d; font-weight: 600;' for _ in row]

            styled_alerts = td.urgent_bin_columns.style.apply(
                highlight_urgent_rows, axis=1
            ).set_table_attributes('class="dataframe urgent-row"')

            # Render Urgent Alerts Table
            st.markdown(styled_alerts.to_html(), unsafe_allow_html=True)

            # Export button for Urgent Alerts
            urgent_excel = to_excel(td.urgent_bin_columns)
            st.download_button(
                label="⬇️ Export Urgent Alerts to Excel",
                data=urgent_excel,
                file_name='Urgent_Alerts.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
