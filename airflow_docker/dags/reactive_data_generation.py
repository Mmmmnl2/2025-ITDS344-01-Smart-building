from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import json

# -----------------------
# CONFIG
# -----------------------
DATA_DIR = "/opt/airflow/data"
TIMETABLE_FILE = f"{DATA_DIR}/timetable.csv"
TIMETABLE_CSV = "/tmp/timetable.csv"
OUTPUT_SENSOR = f"/tmp/sensor_data.csv"

default_args = {
    'owner': 'smart-building',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# -----------------------
# TASK 1: Load Timetable
# -----------------------
def load_timetable():
    import pandas as pd
    import os

    file_path = "/opt/airflow/data/timetable.csv"

    print(f"[DEBUG] Reading CSV: {file_path}")
    print(f"[DEBUG] Exists? {os.path.exists(file_path)}")

    df = pd.read_csv(file_path)

    print("[DEBUG] Preview:")
    print(df.head())

    # df.to_csv(TIMETABLE_CSV, index=False)
    return df.to_dict(orient="records")

# -----------------------
# TASK 2: Expand Timetable
# -----------------------
def expand_timetable():
    timetable = pd.read_csv(TIMETABLE_CSV)

    start_date = datetime(2026, 4, 1)
    end_date = datetime(2026, 4, 7)

    holidays = [
        datetime(2026, 4, 6).date()
    ]

    day_map = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6
    }

    expanded_rows = []

    current = start_date
    while current <= end_date:
        if current.date() in holidays:
            current += timedelta(days=1)
            continue

        for _, row in timetable.iterrows():
            if current.weekday() == day_map[row['day_of_week']]:
                expanded_rows.append({
                    "roomID": row["roomID"],
                    "date": current.date(),
                    "start_time": row["start_time"],
                    "end_time": row["end_time"],
                    "course": row["course"]
                })

        current += timedelta(days=1)

    df = pd.DataFrame(expanded_rows)
    df.to_csv("/opt/airflow/data/timetable_expanded.csv", index=False)

    print(f"[INFO] Expanded timetable rows: {len(df)}")
    print(df.head(10).to_string(index=False))  


# -----------------------
# TASK 3: Reactive data generation
# -----------------------
def reactive_iot_generator():
    import random
    import uuid
    import json
    import time
    import pandas as pd
    from datetime import datetime, timedelta
    from kafka import KafkaConsumer, KafkaProducer

    print("[INFO] Reactive IoT generator started")

    # -----------------------
    # LOAD CSV BASE SCHEDULE
    # -----------------------
    df = pd.read_csv("/opt/airflow/data/timetable_expanded.csv")

    schedule = {}
    for _, row in df.iterrows():
        key = (row['roomID'], str(row['date']))
        schedule.setdefault(key, []).append((row['start_time'], row['end_time']))

    rooms = df['roomID'].unique()

    # -----------------------
    # KAFKA CONFIG
    # -----------------------
    KAFKA_BROKER = "host.docker.internal:9092"  # adjust if needed

    consumer = KafkaConsumer(
        'room_booking',   # ✅ matches your topic
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='iot-generator'
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # -----------------------
    # STATE: BOOKING OVERRIDES
    # -----------------------
    booking_updates = {}

    def is_occupied(room, now):
        date_str = str(now.date())
        time_str = now.strftime("%H:%M:%S")

        # 🔹 Base schedule (CSV)
        base = False
        key = (room, date_str)

        if key in schedule:
            for start, end in schedule[key]:
                if start <= time_str <= end:
                    base = True
                    break

        # 🔹 Kafka booking override
        if key in booking_updates:
            bookings = booking_updates[key]

            for b in bookings:
                if b['start_time'] <= time_str <= b['end_time']:
                    return True

        return base

    # -----------------------
    # START TIME
    # -----------------------
    current = datetime(2026, 4, 1, 8, 0)

    # -----------------------
    # MAIN LOOP
    # -----------------------
    while True:

        # 🔹 Consume bookings (non-blocking)
        msgs = consumer.poll(timeout_ms=100)

        for tp, messages in msgs.items():
            for msg in messages:
                event = msg.value

                # ✅ Map your producer format
                room = event['room']
                date = event['date']

                booking = {
                    "start_time": event['time_start'] + ":00",
                    "end_time": event['time_end'] + ":00"
                }

                key = (room, date)
                booking_updates.setdefault(key, []).append(booking)

                print(f"[BOOKING RECEIVED] {event}")

        # 🔹 Generate sensor data
        for room in rooms:

            occupied = is_occupied(room, current)

            for device_type in ['temperature', 'light', 'power']:

                if device_type == 'temperature':
                    value = random.uniform(22, 26) if occupied else random.uniform(28, 32)
                elif device_type == 'light':
                    value = random.randint(300, 700) if occupied else random.randint(0, 50)
                else:
                    value = random.uniform(2, 4) if occupied else random.uniform(0.2, 0.5)

                data = {
                    "readingID": str(uuid.uuid4()),
                    "timestamp": current.strftime("%Y-%m-%d %H:%M:%S"),
                    "roomID": room,
                    "deviceID": f"{room}_{device_type}",
                    "value": round(value, 2),
                    "deviceType": device_type,
                    "occupied": occupied
                }

                producer.send("iot-sensor", data)

        print(f"[INFO] Generated data at {current}")

        current += timedelta(minutes=5)
        time.sleep(1)

# -----------------------
# DAG DEFINITION
# -----------------------
with DAG(
    dag_id='smart_building_pipeline_test',
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False
) as dag:

    task_load_timetable = PythonOperator(
        task_id='load_timetable',
        python_callable=load_timetable
    )

    task_expand_timetable = PythonOperator(
    task_id='expand_timetable',
    python_callable=expand_timetable
    )

    task_reactive_stream = PythonOperator(
        task_id='reactive_iot_generator',
        python_callable=reactive_iot_generator
    )

    # -----------------------
    # PIPELINE ORDER
    # -----------------------
    task_load_timetable >> task_expand_timetable >> task_reactive_stream
