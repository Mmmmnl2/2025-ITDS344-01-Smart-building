import json
import time
import uuid
import random
import pandas as pd
import os
from kafka import KafkaProducer
from datetime import datetime

# CONFIG
KAFKA_BROKER = "100.110.59.93:9092"
TOPIC_IOT = 'iot-sensor'
CSV_PATH = "/opt/airflow/data/bronze/raw_sensor_data.csv"
TIMETABLE_PATH = "/opt/airflow/data/timetable.csv" # ใช้แค่เอา roomID

def start_iot_stream():
    # 1. ตั้งค่า Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return

    # 2. ดึงแค่รายชื่อ roomID มาจากไฟล์
    if os.path.exists(TIMETABLE_PATH):
        df_rooms = pd.read_csv(TIMETABLE_PATH)
        all_rooms = df_rooms['roomID'].unique().tolist()
    else:
        # ถ้าไม่มีไฟล์ ให้ใช้รายชื่อ Mockup แทน
        all_rooms = ['Lab103', 'Lab104', 'Lab105', 'Lab106']
        print("⚠️ Warning: ไม่พบไฟล์ CSV ใช้รายชื่อห้อง Mockup แทน")

    print(f"🚀 Streamer V3 (Random Mode) is running with {len(all_rooms)} rooms...")

    while True:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")

        # ปัจจัยความเหวี่ยงของค่า (ให้ข้อมูลดูสมจริงขึ้น)
        weather_drift = random.uniform(-3.0, 3.0)
        time_factor = (now.hour - 12)**2 / 5

        sampled_rooms = random.sample(all_rooms, min(2, len(all_rooms)))

        for room in sampled_rooms:
            # สุ่ม "สภาวะ" ภายในห้องเฉยๆ เพื่อใช้เลือกช่วงตัวเลข (แต่ไม่ส่งค่านี้ออกไป)
            internal_occupied_state = random.choice([True, False])

            # 3. Logic สุ่มค่าเซนเซอร์
            sensors_config = {
                'temperature': (22 + weather_drift, 25 + weather_drift) if internal_occupied_state else (28 + time_factor, 32 + time_factor),
                'light': (500, 900) if internal_occupied_state else (0, 80),
                'power': (10.0, 50.0) if internal_occupied_state else (0.1, 0.8),
                'humidity': (40, 55) if internal_occupied_state else (65, 85),
                'co2': (600, 1000) if internal_occupied_state else (300, 450)
            }

            for d_type, (low, high) in sensors_config.items():
                r = random.random()

                # Logic สุ่มค่า Outlier/Null
                if r < 0.05:
                    val = None
                elif r < 0.10: # Extreme Outliers
                    if d_type == 'power': val = round(random.uniform(110, 300), 2)
                    elif d_type == 'co2': val = round(random.uniform(2500, 5000), 2)
                    elif d_type == 'temperature': val = round(random.uniform(150, 500), 2)
                    else: val = round(random.uniform(500, 1000), 2)
                else:
                    val = round(random.uniform(low, high) + random.gauss(0, 0.5), 2)

                # 4. สร้าง Message
                iot_event = {
                    "readingID": str(uuid.uuid4()),
                    "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "roomID": room,
                    "deviceID": f"{room}_{d_type}",
                    "value": val,
                    "deviceType": d_type
                }

                # 5. ส่งเข้า Kafka และ CSV
                producer.send(TOPIC_IOT, iot_event)

                # บันทึก CSV (ตัด Header ถ้าไฟล์มีอยู่แล้ว)
#                 df_iot = pd.DataFrame([iot_event])
#                 df_iot.to_csv(CSV_PATH, mode='a', index=False, header=not os.path.exists(CSV_PATH))

        producer.flush()
        print(f"📡 [SENT] {current_time} | Drift: {weather_drift:.2f} | Rooms: {sampled_rooms}")
        time.sleep(10)

if __name__ == "__main__":
    start_iot_stream()