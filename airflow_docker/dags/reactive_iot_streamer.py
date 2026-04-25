import json
import time
import uuid
import random
import pandas as pd
import os
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

# CONFIG
KAFKA_BROKER = "100.110.59.93:9092"
TOPIC_BOOKING = 'room-booking'
TOPIC_IOT = 'iot-sensor'
CSV_PATH = "/opt/airflow/data/bronze/raw_sensor_data.csv"
TIMETABLE_PATH = "/opt/airflow/data/timetable.csv"

def start_iot_stream():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    consumer = KafkaConsumer(
        TOPIC_BOOKING,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )

    df_raw = pd.read_csv(TIMETABLE_PATH)
    all_rooms = df_raw['roomID'].unique().tolist()
    booking_overrides = {}

    print("🚀 Streamer V2 (Extreme Mode) is running...")

    while True:
        # เช็คการจอง
        msg_pack = consumer.poll(timeout_ms=100)
        for tp, messages in msg_pack.items():
            for msg in messages:
                event = msg.value
                room = event['room']
                booking_overrides.setdefault((room, event['date']), []).append({
                    "start": event['time_start'] + ":00",
                    "end": event['time_end'] + ":00"
                })

        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        current_date = now.strftime("%Y-%m-%d")

        # ปัจจัยความเหวี่ยง (Weather & Time)
        weather_drift = random.uniform(-3.0, 3.0)
        time_factor = (now.hour - 12)**2 / 5

        sampled_rooms = random.sample(all_rooms, min(2, len(all_rooms)))

        for room in sampled_rooms:
            occupied = False
            day_now = now.strftime("%A")
            match = df_raw[(df_raw['roomID'] == room) & (df_raw['day_of_week'] == day_now) &
                           (df_raw['start_time'] <= current_time) & (df_raw['end_time'] >= current_time)]
            if not match.empty: occupied = True
            for b in booking_overrides.get((room, current_date), []):
                if b['start'] <= current_time <= b['end']: occupied = True; break

            # ปรับช่วงค่าให้เหวี่ยงสะใจอาจารย์
            sensors = {
                'temperature': (22 + weather_drift, 25 + weather_drift) if occupied else (28 + time_factor, 32 + time_factor),
                'light': (500, 900) if occupied else (0, 80),
                'power': (3.0, 6.0) if occupied else (0.1, 0.5),
                'humidity': (40, 55) if occupied else (65, 85),
                'co2': (600, 1000) if occupied else (300, 450)
            }

            for d_type, (low, high) in sensors.items():
                r = random.random()
                if r < 0.05: # 5% เป็นค่าว่าง
                    val = None
                elif r < 0.10: # 10% เป็นค่าพุ่ง (Outlier)
                    val = round(random.uniform(500, 1000), 2)
                else:
                    # ใส่ Gauss Noise ให้เลขขยับยิกๆ
                    val = round(random.uniform(low, high) + random.gauss(0, 0.5), 2)

                iot_event = {
                    "readingID": str(uuid.uuid4()),
                    "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "roomID": room,
                    "deviceID": f"{room}_{d_type}",
                    "value": val,
                    "deviceType": d_type,
                    "occupied": occupied
                }

                producer.send(TOPIC_IOT, iot_event)
                df_iot = pd.DataFrame([iot_event])
                df_iot.to_csv(CSV_PATH, mode='a', index=False, header=not os.path.exists(CSV_PATH))

        print(f"📡 [SENT] {current_time} | Drift: {weather_drift:.2f} | Noise added.")
        time.sleep(10)

if __name__ == "__main__":
    start_iot_stream()
