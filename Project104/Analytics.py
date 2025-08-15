import streamlit as st
import Utilities as util

def show_analytics():
    layout= util.double_column()
    a,b= layout

    with a: 
        #Container 1: the bar chart
        with st.container():
            st.subheader("Bin Status")   
        #Container 3: Line Graph
        with st.container():
            st.subheader("Temperature over time")

    with b:
        #Container 2: Pie Chart
        with st.container():
            st.subheader("Bin Status")
        #Container 4: Line Graph
        with st.container():
            st.subheader("Fill Level")




    
    

   

