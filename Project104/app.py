import streamlit as st
from streamlit_option_menu import option_menu
import Dashboard
import Analytics

st.set_page_config(
    page_title="Maribyrnong Smart City Bins",
    initial_sidebar_state="collapsed",
    layout="wide"
)

#sidebar nav
with st.sidebar:
    selected = option_menu(
        menu_title = "Menu",
        options = ["Dashboard", "Analytics"],
        icons = ["nut", "bar-chart"],
        menu_icon ="list",
        default_index=0
    )

#Display content based on selected page
if selected == "Dashboard":
    Dashboard.show_dashboard()

if selected =="Analytics":
    Analytics.show_analytics()