import streamlit as st
import pydeck as pdk
import pandas as pd

def show_dashboard():
    with st.container():
        st.title("Maribyrnong Smart City Bins Dashboard - Project 104")
        st.divider()
    
    with st.container():
        st.header("Real-Time Bin Monitoring Chart")

        view_state = pdk.ViewState(
            latitude=-37.7932,
            longitude=144.8990,
            zoom=17
        )

        bin_locations_layer = pdk.Layer(
            "ScatterPlotLayer",
            data = None, # replace with actual data source
            get_position = "[lng, lat]",
            get_radius = 5,
            pickable = True
        )

        st.pydeck_chart(pdk.Deck(
            layers = [bin_locations_layer],
            initial_view_state=view_state
        ))
