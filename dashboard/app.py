import streamlit as st
from streamlit_autorefresh import st_autorefresh
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
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

selected_metric = st.sidebar.selectbox(
    "Select Metric",
    available_metrics,
    key="metric_select"
)

refresh_rate = st.sidebar.slider(
    "Refresh Rate (sec)",
    2, 30, 5
)

# 🔁 AUTO REFRESH (non-blocking)
st_autorefresh(interval=refresh_rate * 1000, key="auto_refresh")

# --- LOAD DATA ---
cursor = list(collection.find({}))

st.title("🏙️ Smart Building: Operations Dashboard")

if cursor:
    df = pd.DataFrame(cursor)
    df['load_date'] = pd.to_datetime(df['load_date'])

    # sanity filter
    df = df[df['avg_value'] < 1000]

    # --- ROOM FILTER ---
    available_rooms = sorted(df['roomid'].dropna().unique())

    selected_room = st.sidebar.selectbox(
        "Select Room (optional)",
        ["All"] + list(available_rooms),
        key="room_select"
    )

    # --- FILTERED DATA ---
    filtered_df = df[df['devicetype'] == selected_metric]

    if selected_room != "All":
        filtered_df = filtered_df[filtered_df['roomid'] == selected_room]

    # =========================
    # KPI CARDS
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
    # 🚨 ALERTS
    # =========================
    st.subheader("🚨 Alerts")

    thresholds = {
        "temperature": 30,
        "co2": 1000,
        "power": 500,
        "humidity": 70
    }

    if selected_metric in thresholds:
        alert_df = df[
            (df['devicetype'] == selected_metric) &
            (df['avg_value'] > thresholds[selected_metric])
        ]

        if not alert_df.empty:
            st.error(f"⚠️ {len(alert_df)} abnormal readings detected!")
            st.dataframe(
                alert_df[['roomid', 'avg_value', 'load_date']],
                width='stretch'
            )
        else:
            st.success("✅ No abnormal readings")

    st.divider()

    # =========================
    # 🔥 HEATMAP (ONLY ALL)
    # =========================
    if selected_room == "All":
        st.subheader(f"🔥 Heatmap: {selected_metric}")

        heatmap_df = filtered_df.pivot_table(
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

            st.plotly_chart(fig_heatmap, width='stretch')

        st.divider()

    # =========================
    # 📈 TIME SERIES
    # =========================
    st.subheader("📈 Time Series Trend")

    if not filtered_df.empty:
        fig_ts = px.line(
            filtered_df.sort_values("load_date"),
            x="load_date",
            y="avg_value",
            color="roomid" if selected_room == "All" else None,
            markers=True,
            title=f"{selected_metric} over Time"
        )

        st.plotly_chart(fig_ts, width='stretch')

    st.divider()

    # =========================
    # 🏆 RANKING (ONLY ALL)
    # =========================
    if selected_room == "All":
        st.subheader(f"🏆 Room Ranking: {selected_metric}")

        rank_df = (
            df[df['devicetype'] == selected_metric]
            .groupby("roomid")['avg_value']
            .mean()
            .reset_index()
            .sort_values(by="avg_value", ascending=False)
        )

        if not rank_df.empty:
            fig_rank = px.bar(
                rank_df,
                x="roomid",
                y="avg_value",
                color="avg_value",
                title=f"Average {selected_metric} by Room"
            )

            st.plotly_chart(fig_rank, width='stretch')


    # =========================
    # 📋 SNAPSHOT TABLE
    # =========================
    st.subheader("📋 Latest Snapshot per Room")

    latest_df = (
        df.sort_values("load_date")
          .groupby(["roomid", "devicetype"])
          .tail(1)
    )

    snapshot_view = latest_df.pivot(
        index="roomid",
        columns="devicetype",
        values="avg_value"
    )

    st.dataframe(snapshot_view, width='stretch')

    st.divider()
    
    # =========================
    # FOOTER
    # =========================
    st.caption(f"Last Sync: {datetime.now().strftime('%H:%M:%S')}")

else:
    st.warning("No data available in MongoDB.")
