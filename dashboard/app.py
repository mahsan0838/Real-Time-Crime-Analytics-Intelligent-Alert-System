import streamlit as st
import pandas as pd
import psycopg2
from pymongo import MongoClient
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.runtime import get_mongo, get_postgres, load_config

st.set_page_config(page_title="Crime Analytics", layout="wide")

st.title("🚨 Real-Time Crime Analytics & Intelligent Alert System")

def load_data():
    cfg = load_config()
    pg = get_postgres(cfg)
    mongo = get_mongo(cfg)

    conn = psycopg2.connect(
        host=pg.host,
        port=pg.port,
        database=pg.database,
        user=pg.user,
        password=pg.password,
    )
    # Get total count
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM alerts")
    total_alerts = cur.fetchone()[0]
    
    # Get latest 100 for display
    alerts = pd.read_sql("SELECT * FROM alerts ORDER BY timestamp DESC LIMIT 100", conn)
    arrest_rates = pd.read_sql("SELECT * FROM arrest_rates ORDER BY arrest_rate DESC LIMIT 15", conn)
    conn.close()
    
    # MongoDB count
    try:
        client = MongoClient(mongo.host, mongo.port)
        db = client[mongo.database]
        mongo_count = db[mongo.collection_alerts].count_documents({})
        client.close()
    except:
        mongo_count = 0
    
    return alerts, arrest_rates, total_alerts, mongo_count

# Load fresh data on every run (no cache)
alerts_df, arrest_df, pg_total, mongo_total = load_data()

# Metrics
col1, col2, col3 = st.columns(3)
col1.metric("Total Alerts (PostgreSQL)", pg_total)
col2.metric("MongoDB Alerts", mongo_total)
col3.metric("Latest District", alerts_df.iloc[0]['district'] if not alerts_df.empty else "None")

st.markdown("---")

# Auto-refresh checkbox
auto_refresh = st.checkbox("Auto-refresh every 10 seconds", value=False)

if auto_refresh:
    st.empty()
    import time
    time.sleep(10)
    st.rerun()

# Refresh button
if st.button("🔄 Refresh Now"):
    st.rerun()

st.markdown("---")

# Charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("Alerts by Hour")
    if not alerts_df.empty:
        alerts_df['hour'] = pd.to_datetime(alerts_df['timestamp']).dt.hour.astype(str)
        hourly = alerts_df.groupby('hour').size().reset_index(name='count')
        fig = px.bar(hourly, x='hour', y='count', title="Alerts by Hour of Day", text_auto=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No alerts")

with col2:
    st.subheader("Alerts by District")
    if not alerts_df.empty:
        alerts_df['district'] = alerts_df['district'].astype(str)
        district = alerts_df.groupby('district').size().reset_index(name='count')
        fig = px.bar(district, x='district', y='count', title="Total Alerts per District (Last 100)", text_auto=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No alerts")

st.markdown("---")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Arrest Rates by Crime Type")
    if not arrest_df.empty:
        fig = px.bar(arrest_df.head(10), x='crime_type', y='arrest_rate', title="Top 10 Arrest Rates")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Run populate_tables.py first")

with col2:
    st.subheader("Recent Alerts (Last 10)")
    if not alerts_df.empty:
        st.dataframe(alerts_df[['district', 'event_count', 'timestamp']].head(10), use_container_width=True)

st.markdown("---")
st.caption("Click Refresh Now or enable auto-refresh. Keep producer and anomaly_detector running.")