import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
import time
from datetime import datetime

# --- CONFIG ---
st.set_page_config(page_title="Smart Building Ops", layout="wide")

@st.cache_resource
def get_db_client():
    return MongoClient("mongodb://localhost:27017/")

client = get_db_client()
db = client["smart_building"]
collection = db["fact_iot_readings"]

# --- SIDEBAR ---
try:
    available_metrics = sorted(collection.distinct("devicetype"))
except:
    available_metrics = ["temperature", "co2", "power", "humidity", "light"]

selected_metric = st.sidebar.selectbox("Select Metric", available_metrics)
refresh_rate = st.sidebar.slider("Refresh Rate (sec)", 2, 30, 5)

# --- MAIN ---
st.title("🏙️ Smart Building: Operations Dashboard")
dashboard = st.empty()

if 'count' not in st.session_state:
    st.session_state.count = 0

# --- LOOP ---
while True:
    cursor = list(collection.find({}))
    
    if cursor:
        df = pd.DataFrame(cursor)
        df['load_date'] = pd.to_datetime(df['load_date'])

        # Optional sanity filter (recommended)
        df = df[df['avg_value'] < 1000]

        with dashboard.container():

            # =========================
            # 1. KPI CARDS
            # =========================
            st.subheader("📊 Key Metrics")

            col1, col2, col3, col4 = st.columns(4)

            def safe_avg(dtype):
                vals = df[df['devicetype'] == dtype]['avg_value']
                return vals.mean() if not vals.empty else 0

            col1.metric("🌡 Avg Temp", f"{safe_avg('temperature'):.1f} °C")
            col2.metric("💨 Avg CO2", f"{safe_avg('co2'):.0f} ppm")
            col3.metric("⚡ Total Power", f"{df[df['devicetype']=='power']['avg_value'].sum():.2f}")
            col4.metric("🏢 Rooms", df['roomid'].nunique())

            st.divider()

            # =========================
            # 2. HEATMAP (Filtered by metric)
            # =========================
            st.subheader(f"🔥 Heatmap: {selected_metric}")

            heat_df = df[df['devicetype'] == selected_metric]

            heatmap_df = heat_df.pivot_table(
                index="roomid",
                values="avg_value",
                aggfunc="mean"
            )

            if not heatmap_df.empty:
                fig_heatmap = px.imshow(
                    heatmap_df,
                    text_auto=True,
                    aspect="auto",
                    color_continuous_scale="RdYlGn_r",
                    title=f"{selected_metric} across rooms"
                )

                st.plotly_chart(
                    fig_heatmap,
                    width='stretch',
                    key=f"heat_{st.session_state.count}"
                )

            st.divider()

            # =========================
            # 3. TIME SERIES (Filtered by metric) 
            # =========================
            st.subheader("📈 Time Series Trend")

            ts_df = df[df['devicetype'] == selected_metric]

            if not ts_df.empty:
                fig_ts = px.line(
                    ts_df.sort_values("load_date"),
                    x="load_date",
                    y="avg_value",
                    color="roomid",
                    markers=True,
                    title=f"{selected_metric} over Time"
                )

                st.plotly_chart(
                    fig_ts,
                    width='stretch',
                    key=f"ts_{st.session_state.count}"
                )

            st.divider()

            # =========================
            # 4. BAR CHART (Filtered by metric)
            # =========================
            st.subheader(f"🏢 Room Comparison: {selected_metric}")

            bar_df = df[df['devicetype'] == selected_metric].sort_values(by="avg_value")

            if not bar_df.empty:
                fig_bar = px.bar(
                    bar_df,
                    x="roomid",
                    y="avg_value",
                    color="roomid",
                    title=f"{selected_metric} by Room"
                )

                st.plotly_chart(
                    fig_bar,
                    width='stretch',
                    key=f"bar_{st.session_state.count}"
                )

            # =========================
            # FOOTER
            # =========================
            st.caption(f"Last Sync: {datetime.now().strftime('%H:%M:%S')}")

    st.session_state.count += 1
    time.sleep(refresh_rate)
