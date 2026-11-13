"""Receive live Hamburg-area AIS reports and publish them to Kafka."""
import asyncio
import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer
from websockets.asyncio.client import connect

load_dotenv()
API_KEY = os.getenv("AISSTREAM_API_KEY")
TOPIC = "vessel-positions"
AREA = [[[53.35, 9.30], [53.95, 10.35]]]
TYPES = ("PositionReport", "StandardClassBPositionReport")


async def main():
    if not API_KEY:
        raise ValueError("Set AISSTREAM_API_KEY in .env")

    subscription = {
        "APIKey": API_KEY,
        "BoundingBoxes": AREA,
        "FilterMessageTypes": list(TYPES),
    }

    while True:  # Reconnect after a temporary failure
        producer = None
        try:
            producer = await asyncio.to_thread(
                KafkaProducer,
                bootstrap_servers="localhost:9092",
                value_serializer=lambda x: json.dumps(x).encode(),
                acks="all",
            )
            async with connect(
                "wss://stream.aisstream.io/v0/stream",
                compression="deflate",
                open_timeout=20,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                await ws.send(json.dumps(subscription))
                print("Connected to AISStream and Kafka")

                async for raw in ws:
                    data = json.loads(raw)
                    kind = data.get("MessageType")
                    if kind not in TYPES:
                        continue
                    info = data.get("MetaData") or {}
                    position = data.get("Message", {}).get(kind) or {}
                    lat, lon, mmsi = (
                        position.get("Latitude"),
                        position.get("Longitude"),
                        info.get("MMSI"),
                    )
                    if mmsi is None or lat is None or lon is None:
                        continue
                    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                        continue

                    report = {
                        "mmsi": str(mmsi),
                        "ship_name": info.get("ShipName"),
                        "latitude": lat,
                        "longitude": lon,
                        "speed_knots": position.get("Sog"),
                        "source_time": info.get("time_utc"),
                        "received_at": datetime.now(timezone.utc).isoformat(),
                    }
                    sent = producer.send(TOPIC, value=report)
                    await asyncio.to_thread(sent.get, timeout=35)
                    print("Sent to Kafka:", report["ship_name"], "|", report["mmsi"])

        except Exception as error:
            print(f"Connection error: {error}. Retrying in 5 seconds...")
        finally:
            if producer:
                try:
                    await asyncio.to_thread(producer.close, timeout=5)
                except Exception as error:
                    print("Producer close warning:", error)
        await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Producer stopped")
