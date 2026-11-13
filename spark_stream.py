"""Process AIS Kafka reports with Spark and safely save them to SQLite."""
import hashlib
import json
import sqlite3
import time

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructField, StructType, StringType, DoubleType

DB = "/opt/project/vessels.db"
spark = SparkSession.builder.appName("HamburgPort").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("mmsi", StringType()),
    StructField("ship_name", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed_knots", DoubleType()),
    StructField("source_time", StringType()),
    StructField("received_at", StringType()),
])

vessels = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "vessel-positions")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING) AS json", "partition", "offset")
    .select(from_json(col("json"), schema).alias("ship"), "partition", "offset")
    .select("ship.*", "partition", "offset")
    .filter(col("mmsi").isNotNull() & col("latitude").between(-90, 90) & col("longitude").between(-180, 180))
    .withColumn("speed_alert", when(col("speed_knots") > 12, 1).otherwise(0))
)


def observation_key(row):
    """Same original observation -> same key. No timestamp -> no key."""
    timestamp = row["source_time"]
    if not timestamp or not timestamp.strip():
        return None
    values = [row["mmsi"], timestamp.strip(), row["latitude"], row["longitude"], row["speed_knots"]]
    text = json.dumps(values, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def prepare_db(db):
    db.execute("""CREATE TABLE IF NOT EXISTS vessels (
        partition_id INTEGER, kafka_offset INTEGER, mmsi TEXT, ship_name TEXT,
        latitude REAL, longitude REAL, speed_knots REAL, received_at TEXT,
        speed_alert INTEGER, source_time TEXT, observation_key TEXT,
        PRIMARY KEY (partition_id, kafka_offset))""")
    columns = {item[1] for item in db.execute("PRAGMA table_info(vessels)")}
    for name in ("source_time", "observation_key", "saved_at"):
        if name not in columns:
            db.execute(f"ALTER TABLE vessels ADD COLUMN {name} TEXT")
    db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_observation
                  ON vessels(observation_key) WHERE observation_key IS NOT NULL""")
    db.commit()


def save_batch(batch, batch_id):
    rows = batch.collect()
    if not rows:
        return

    records = [(
        r["partition"], r["offset"], r["mmsi"], r["ship_name"],
        r["latitude"], r["longitude"], r["speed_knots"],
        r["received_at"], r["speed_alert"], r["source_time"], observation_key(r)
    ) for r in rows]

    for attempt in range(1, 6):
        db = None
        try:
            db = sqlite3.connect(DB, timeout=30)
            prepare_db(db)
            db.execute("BEGIN IMMEDIATE")
            saved_at = datetime.now(timezone.utc).isoformat()
            before = db.total_changes
            db.executemany("""
    INSERT OR IGNORE INTO vessels (
        partition_id, kafka_offset, mmsi, ship_name,
        latitude, longitude, speed_knots, received_at,
        speed_alert, source_time, observation_key, saved_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", [r + (saved_at,) for r in records])
            saved = db.total_changes - before
            db.commit()
            print(f"Database write successful on attempt {attempt}")
            break
        except sqlite3.OperationalError as error:
            if db:
                try:
                    db.rollback()
                except sqlite3.Error:
                    pass
            print(f"Database write failed ({attempt}/5): {error}")
            if attempt == 5:
                raise  # Spark can replay this batch on restart
            time.sleep(min(2 ** attempt, 16))
        finally:
            if db:
                db.close()

    speeds = [r["speed_knots"] for r in rows if r["speed_knots"] is not None]
    print(f"\nBatch {batch_id}")
    print(f"Vessel reports received: {len(rows)}")
    print(f"New reports saved: {saved}")
    print(f"Repeated records skipped: {len(rows) - saved}")
    print(f"Average speed: {sum(speeds)/len(speeds) if speeds else 0:.2f} knots")
    print(f"Speed alerts: {sum(r['speed_alert'] for r in rows)}")
    print("Saved to SQLite")


query = (vessels.writeStream.foreachBatch(save_batch)
         .option("checkpointLocation", "/opt/project/spark-checkpoint").start())
query.awaitTermination()
