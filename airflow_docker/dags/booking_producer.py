import json
import time
import random
import pandas as pd
from datetime import datetime, timedelta # <--- เพิ่ม timedelta
from kafka import KafkaProducer

KAFKA_BROKER = "100.110.59.93:9092"
TOPIC_NAME = "room-booking" # (เช็คชื่อ Topic ให้ตรงกับฝั่ง Consumer ด้วยนะครับ)
TIMETABLE_PATH = "/opt/airflow/data/timetable.csv"

def generate_booking():
    df = pd.read_csv(TIMETABLE_PATH)
    rooms = df['roomID'].unique().tolist()

    room = random.choice(rooms)
    start_h = random.randint(8, 16)

    # 🔹 LOGIC ใหม่: สุ่มว่าจะเป็นการจองวันนี้ หรือ จองล่วงหน้า
    if random.random() < 0.7:
        # โอกาส 70% จองวันนี้ (เพื่อให้เห็นผลทันทีตอนพรีเซนต์)
        booking_date = datetime.now()
    else:
        # โอกาส 30% จองล่วงหน้า 1-3 วัน (เพื่อความสมจริง)
        booking_date = datetime.now() + timedelta(days=random.randint(1, 3))

    return {
        "room": room,
        "date": booking_date.strftime("%Y-%m-%d"),
        "time_start": f"{start_h:02d}:00",
        "time_end": f"{start_h+2:02d}:00",
        "booking_id": f"BK-{random.randint(1000, 9999)}"
    }

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("📖 เริ่มจำลองการจองห้อง...")
try:
    while True:
        data = generate_booking()
        producer.send(TOPIC_NAME, data)
        # ปรับเวลาแสดงผลนิดหน่อยให้ดูชัดเจน
        print(f"✅ จองห้อง {data['room']} | วันที่: {data['date']} | เวลา: {data['time_start']} - {data['time_end']}")
        time.sleep(random.randint(20, 60))
except KeyboardInterrupt:
    producer.close()