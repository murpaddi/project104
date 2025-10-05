import streamlit as st
import pydeck as pdk
import pandas as pd

def double_column():
    column1, _, column2= st.columns([1, .1, 1])
    return column1,column2

def remove_elements():
        st.markdown(
        """
            <style>
            [data-testid="stElementToolbar"] {
            display: none;
            }
            </style>
        """,
        unsafe_allow_html=True
    )


def load_map(data):
    view_state = pdk.ViewState(
            latitude=-37.7932,
            longitude=144.8990,
            zoom=17
        )
    
    bin_locations_layer = pdk.Layer(
            "ScatterplotLayer",
            data = data, # replace with actual data source
            get_position = ['Lng', 'Lat'],
            get_color = [255, 0, 0, 160],
            get_radius = 4,
            pickable = True
        )
    deck = pdk.Deck(
            layers = [bin_locations_layer],
            initial_view_state=view_state,
            tooltip={
                "text": 
                "Bin ID: {BinID}\n "
                "Fill: {Fill}\n "
                "Temp: {Temp}\n "
                "Battery: {Battery}%"
                }
        )
    
    return deck