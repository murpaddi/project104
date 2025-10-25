import streamlit as st
from streamlit_option_menu import option_menu
from View import Dashboard, Analytics

st.set_page_config(
    page_title="Maribyrnong Smart City Bins",
    initial_sidebar_state="expanded",
    layout="wide"
)

#sidebar nav
with st.sidebar:
    selected = option_menu(
        menu_title = "Menu",
        options = ["Dashboard", "Analytics"],
        icons = ["map", "bar-chart"],
        menu_icon ="list",
        default_index=0
    )

#Display content based on selected page

#Ensure page placeholders always exist
if "dash_root" not in st.session_state or st.session_state.get("dash_root") is None:
    st.session_state["dash_root"] = st.empty()

if "ana_root" not in st.session_state or st.session_state.get("ana_root") is None:
    st.session_state["ana_root"] = st.empty()



#Render selected page; clear the other one
if selected == "Dashboard":
    # Always re-create placeholder before using it
    if not st.session_state.get("dash_root"):
        st.session_state["dash_root"] = st.empty()
    st.session_state["ana_root"].empty()     # Safe now: guaranteed to exist
    with st.session_state["dash_root"].container():
        Dashboard.show_dashboard()

elif selected == "Analytics":
    if not st.session_state.get("ana_root"):
        st.session_state["ana_root"] = st.empty()
    st.session_state["dash_root"].empty()
    with st.session_state["ana_root"].container():
        Analytics.show_analytics()