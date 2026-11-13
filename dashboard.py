"""Live Hamburg vessel activity, history, and estimated port entries."""
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

DB = Path(__file__).with_name("vessels.db")
st.set_page_config(page_title="Hamburg Port Monitor", page_icon="🚢", layout="wide")
st.title("🚢 Hamburg Port Vessel Monitoring")
st.caption("Live AIS reports • Kafka • Spark • SQLite")
st.info("AIS observations only — not an official port traffic-control system.")

# Approximate project boundary, not Hamburg's official port boundary.
ENTRIES_SQL = """
WITH movements AS (
    SELECT mmsi,
      CASE WHEN latitude BETWEEN 53.47 AND 53.56
             AND longitude BETWEEN 9.80 AND 10.05 THEN 1 ELSE 0 END AS inside,
      LAG(CASE WHEN latitude BETWEEN 53.47 AND 53.56
                 AND longitude BETWEEN 9.80 AND 10.05 THEN 1 ELSE 0 END)
          OVER (PARTITION BY mmsi ORDER BY received_at, partition_id, kafka_offset) AS previous
    FROM vessels WHERE mmsi IS NOT NULL
)
SELECT COUNT(*), COUNT(DISTINCT mmsi)
FROM movements WHERE inside = 1 AND previous = 0
"""


@st.fragment(run_every="5s")
def live_dashboard():
    if not DB.exists():
        st.warning("Database missing. Start Spark first.")
        return

    now = datetime.now(timezone.utc)
    minutes_15 = (now - timedelta(minutes=15)).isoformat()
    one_minute = (now - timedelta(minutes=1)).isoformat()
    hours_24 = (now - timedelta(hours=24)).isoformat()

    try:
        with closing(sqlite3.connect(DB, timeout=10)) as db:
            total, unique = db.execute("SELECT COUNT(*), COUNT(DISTINCT mmsi) FROM vessels").fetchone()
            last_minute = db.execute("SELECT COUNT(*) FROM vessels WHERE received_at >= ?", (one_minute,)).fetchone()[0]
            entries, entrants = db.execute(ENTRIES_SQL).fetchone()
            recent = pd.read_sql_query("""SELECT mmsi, ship_name, latitude, longitude,
                speed_knots, received_at FROM vessels WHERE received_at >= ?
                ORDER BY received_at DESC, partition_id DESC, kafka_offset DESC""",
                db, params=(minutes_15,))
            history = pd.read_sql_query("SELECT received_at FROM vessels WHERE received_at >= ?",
                                        db, params=(hours_24,))
    except (sqlite3.Error, pd.errors.DatabaseError) as error:
        st.error(f"Database error: {error}")
        return

    latest = recent.drop_duplicates("mmsi")
    speeds = pd.to_numeric(latest["speed_knots"], errors="coerce").fillna(0)

    a, b, c, d = st.columns(4)
    a.metric("Recently detected ships", len(latest))
    b.metric("Ships moving", int((speeds > 0.5).sum()))
    c.metric("Speed alerts", int((speeds > 12).sum()))
    d.metric("Reports in last minute", last_minute)
    a, b = st.columns(2)
    a.metric("Unique ships recorded", unique)
    b.metric("Total reports stored", total)
    st.caption("Recently detected = reported in the last 15 minutes, not confirmed present in port.")

    st.subheader("⚓ Port Entry Statistics")
    a, b = st.columns(2)
    a.metric("Detected port entries", entries)
    b.metric("Different vessels entering", entrants)
    st.caption("Estimated boundary crossings since data collection began — not official arrivals or a full-year count.")

    st.subheader("📍 Latest vessel positions")
    if not latest.empty:
        st.map(latest[["latitude", "longitude"]].dropna(), latitude="latitude", longitude="longitude", zoom=10)
    else:
        st.info("No vessel reports in the last 15 minutes.")

    st.subheader("📊 Vessel reports over the last 24 hours")
    if not history.empty:
        times = pd.to_datetime(history["received_at"], utc=True, errors="coerce").dropna()
        st.bar_chart(times.dt.floor("h").value_counts().sort_index().rename("Reports"))
    else:
        st.info("No reports in the last 24 hours.")

    st.subheader("🚢 Latest vessel information")
    if not latest.empty:
        table = latest.rename(columns={
            "ship_name": "Ship name", "mmsi": "MMSI", "latitude": "Latitude",
            "longitude": "Longitude", "speed_knots": "Speed (knots)",
            "received_at": "Last report (UTC)",
        })
        table["Ship name"] = table["Ship name"].fillna("").str.strip().replace("", "Unknown vessel")
        st.dataframe(table, width="stretch", hide_index=True)

    st.caption("Last refresh (UTC): " + now.strftime("%Y-%m-%d %H:%M:%S"))


live_dashboard()
