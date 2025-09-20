import streamlit as st
import pydeck as pdk
import pandas as pd
import Utilities as util
import testing_data as td

def show_dashboard():
    
    util.remove_elements()

    with st.container():
        st.title("Maribyrnong Smart City Bins Dashboard - Project 104")
        st.divider()
    
    with st.container():
        st.header("Real-Time Bin Monitoring Map")
        deck_obj = util.load_map(td.map_coords) # Replace with actual data source
        st.pydeck_chart(deck_obj)

    with st.container():
        st.header("Bin Data Overview")

        col1, col2 = util.double_column()
        with col1:
            st.subheader("Bin Status Summary Table")
            st.dataframe(td.bin_data) # Replace with actual data source
            
        with col2:
            st.subheader("Urgent Alerts")
            st.dataframe(td.urgent_bin_columns) # Replace with actual data source