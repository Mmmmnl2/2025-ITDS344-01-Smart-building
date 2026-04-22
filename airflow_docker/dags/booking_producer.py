import json
import time
import random
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "100.110.59.93:9092"
TOPIC_NAME = "room_booking"
TIMETABLE_PATH = "/opt/airflow/data/timetable.csv"

def generate_booking():
    df = pd.read_csv(TIMETABLE_PATH)
    rooms = df['roomID'].unique().tolist()

    room = random.choice(rooms)
    start_h = random.randint(8, 16)

    return {
        "room": room,
        "date": datetime.now().strftime("%Y-%m-%d"),
        "time_start": f"{start_h:02d}:00",
        "time_end": f"{start_h+2:02d}:00",
        "booking_id": f"BK-{random.randint(1000, 9999)}"
    }

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 1) # Force API version if broker doesn't advertise properly
)

print("📖 เริ่มจำลองการจองห้อง...")
try:
    while True:
        data = generate_booking()
        producer.send(TOPIC_NAME, data)
        print(f"✅ จองห้อง {data['room']} เวลา {data['time_start']} - {data['time_end']}")
        time.sleep(random.randint(20, 60)) # นาน ๆ จองที
except KeyboardInterrupt:
    producer.close()
