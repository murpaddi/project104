import streamlit as st
import pydeck as pdk
import pandas as pd
from io import BytesIO
from pathlib import Path
from Model.data_loader import load_live_with_coords
import time

def double_column():
    column1, _, column2= st.columns([1, .05, 1])
    return column1,column2

def two_to_one():
    column1, _, column2 = st.columns([2, 0.05, 1])
    return column1,column2

def remove_elements():
        st.markdown(
        """
            <style>
            [data-testid="stElementToolbar"] {
            display: none;
            }

            div[data-testid="stStatusWidget"] { display: none !important; }
            #stDecoration { display: none !important; }
            footer { visibility: hidden !important; }
            </style>
        """,
        unsafe_allow_html=True  
    )


def load_map(data: pd.DataFrame) -> pdk.Deck:
    if data is None or data.empty:
        return pdk.Deck()
    
    df = data.copy()
    if "Fill" not in df.columns:
        st.warning("No Fill column found for map display.")
        return pdk.Deck()

    import numpy as np
    fill = pd.to_numeric(df["Fill"], errors="coerce").fillna(0).clip(0, 100)
    ratio = fill / 100.0

    df["color"] = [[int(r * 255), int((1 - r) * 255), 0, 200] for r in ratio]

    view_state = pdk.ViewState(
            latitude=-37.7932,
            longitude=144.8990,
            zoom=17
        )
    
    bin_locations_layer = pdk.Layer(
            "ScatterplotLayer",
            data = df,
            get_position = ['Lng', 'Lat'],
            get_fill_color = "color",
            get_radius = 4,
            pickable = True
        )
    
    tooltip = {
        "html": (
            "<b>Bin ID:</b> {BinID}<br/>"
            "<b>Fill:</b> {Fill}%<br/>"
            "<b>Temperature:</b> {Temperature}°C<br/>"
            "<b>Battery:</b> {Battery}V"
        ),
        "style": {"backgroundColor": "rgba(255, 255, 255, 0.8)", "color": "black"},
    }

    return pdk.Deck(
            layers = [bin_locations_layer],
            initial_view_state=view_state,
            tooltip=tooltip
        )

def render_table(df: pd.DataFrame, *, use_container_width=True, height=300):
    if df is None or df.empty:
        st.info("No data available.")
        return
    st.dataframe(df, use_container_width=use_container_width, height=height)

def get_latest_df(show_errors: bool = True) -> pd.DataFrame:
    try:
        return _cached_load()
    except FileNotFoundError:
        if show_errors:
            st.error("No data found in the database yet.")
        return pd.DataFrame()
    except Exception as e:
        if show_errors:
            st.error(f"Error loading data: {e}")
        return pd.DataFrame()
    
def load_bin_log(device_id: str) -> pd.DataFrame:
    """Load historical readings for a single bin from the DB archive"""
    from Model.data_loader import load_archive_with_coords
    try:
        df = load_archive_with_coords(device_id)
        if df.empty:
            return df
        
        need = ["Timestamp", "Fill", "Temperature", "Battery"]
        for c in need:
            if c not in df.columns:
                df[c] = pd.NA
        return df[need + [c for c in df.columns if c not in need]]
    except Exception:
        return pd.DataFrame()


    except Exception:
        return pd.DataFrame()
    
def prep_map_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    map_df = df.reset_index().copy()
    if "Latitude" in map_df.columns and "Longitude" in map_df.columns:
        map_df = map_df.rename(columns={"Latitude": "Lat", "Longitude":"Lng"})
    return map_df


def df_to_excel_bytes(df: pd.DataFrame) -> bytes:
    """Return a bytes payload for an Excel download from a DataFrame."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def download_button_from_df(df: pd.DataFrame, filename: str, label: str, *, key: str | None = None):
    """Render a standard download button for a DataFrame as Excel."""
    disabled = (df is None) or df.empty
    data = b"" if disabled else df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=label,
        data=data,
        file_name=filename,
        mime='text/csv',
        disabled=disabled,
        key=key
    )

def render_map_section(map_data: pd.DataFrame):
    st.subheader("Real Time Bin Monitoring Map")
    st.pydeck_chart(load_map(map_data))

def filter_urgent(df: pd.DataFrame, *, fill_thresh: int=85, temp_thresh: int=40, battery_thresh: float=3.2):
    if df is None or df.empty:
        return df

    snap = df.reset_index().copy()

    if "Temperature" not in snap.columns and "Temp" in snap.columns:
        snap = snap.rename(columns={"Temp": "Temperature"})

    snap["Fill"] = pd.to_numeric(snap.get("Fill"), errors="coerce")
    snap["Temperature"] = pd.to_numeric(snap.get("Temperature") or snap.get("Temp"), errors="coerce")
    snap["Battery"] = pd.to_numeric(snap.get("Battery"), errors="coerce")

    # Base urgent mask
    mask = (
        (snap["Fill"] >= fill_thresh) | 
        (snap["Temp"] >= temp_thresh) | 
        (snap["Battery"] <= battery_thresh)
    )

    urgent = snap[mask].copy()
    if urgent.empty:
        return urgent

    def classify(row):

        if pd.isna(row["Fill"]) and pd.isna(row["Temp"]) and pd.isna(row["Battery"]):
            return "No sensor response"
        
        if pd.notna(row["Fill"]) and row["Fill"] >= 100:
            return "Overflowing"
        
        if pd.notna(row["Fill"]) and row["Fill"] >= fill_thresh:
            return "Approaching full"
        
        if pd.notna(row["Temperature"]) and row["Temperature"] >= temp_thresh:
            return "Heat Warning"
        
        if pd.notna(row["Battery"]) and row["Battery"] <= battery_thresh:
            return "Low Battery"
        return "Needs attention"

    urgent["Alert"] = urgent.apply(classify, axis=1)
    return urgent

@st.cache_data(ttl=2)
def _cached_load():
    return load_live_with_coords()

def refresh_button(label: str = "Refresh Now", key: str | None = None) -> bool:
    with st.sidebar:
        return st.button(label, key=key)

def auto_refresh_controls(key_prefix=""):
    with st.sidebar:
        enabled = st.toggle("Auto-refresh", value=True, key=f"{key_prefix}auto_refresh_enabled")
        interval = st.slider("Refresh interval (sec)", 2, 60, 2, key=f"{key_prefix}auto_refresh_interval")
        st.caption("Dashboard will re-run periodically while enabled.")
    return enabled, interval

def maybe_autorefresh(enabled: bool, interval_sec: int):
    if not enabled:
        return
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        ctx = get_script_run_ctx()
        if ctx is None:
            return
        time.sleep(interval_sec)
        st.rerun()
    except Exception as e:
        st.warning(f"Auto-refresh skipped: {e}")