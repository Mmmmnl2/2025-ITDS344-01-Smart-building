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
# TASK 3: Generate IoT Data
# -----------------------
def generate_iot_data():
    import random
    import uuid

    # --- Imperfection controls ---
    NULL_PROB = 0.02      # 2% missing
    OUTLIER_PROB = 0.01   # 1% extreme values

    df = pd.read_csv("/opt/airflow/data/timetable_expanded.csv")

    start_time = datetime(2026, 4, 1, 8, 0)
    end_time = datetime(2026, 4, 1, 18, 0)

    delta = timedelta(minutes=5)
    current = start_time

    results = []

    while current <= end_time:
        for _, row in df.iterrows():
            room = row['roomID']

            occupied = (
                str(current.date()) == str(row['date']) and
                row['start_time'] <= current.strftime("%H:%M:%S") <= row['end_time']
            )

            for device_type in ['temperature', 'light', 'power']:

                # --- Generate base value ---
                if device_type == 'temperature':
                    base_value = random.uniform(22, 26) if occupied else random.uniform(28, 32)
                elif device_type == 'light':
                    base_value = random.randint(300, 700) if occupied else random.randint(0, 50)
                else:
                    base_value = random.uniform(2, 4) if occupied else random.uniform(0.2, 0.5)

                # --- Apply NULL ---
                r = random.random()

                if r < NULL_PROB:
                    value = None

                # --- Apply OUTLIER ---
                elif r < NULL_PROB + OUTLIER_PROB:
                    if device_type == 'temperature':
                        value = round(random.uniform(50, 100), 2)
                    elif device_type == 'light':
                        value = random.randint(1000, 5000)
                    else:
                        value = round(random.uniform(10, 50), 2)

                # --- Normal value ---
                else:
                    value = round(base_value, 2)

                results.append({
                    "readingID": str(uuid.uuid4()),
                    "timestamp": current,
                    "roomID": room,
                    "deviceID": f"{room}_{device_type}",
                    "value": value,
                    "deviceType": device_type
                })

        current += delta

    df_result = pd.DataFrame(results)
    df_result.to_csv(OUTPUT_SENSOR, index=False)

    print(f"[INFO] Sensor data rows: {len(df_result)}")
    print(df_result.head(15).to_string(index=False))  

    output_path = "/tmp/sensor_data.csv"
    df_result.to_csv(output_path, index=False)

    return output_path

# -----------------------
# TASK 4: Stream Data to Kafka in itds344@10.34.111.33
# -----------------------
def stream_to_kafka():
    import time
    import subprocess
    # import json

    file_path = "/tmp/sensor_data.csv"

    # SSH target
    SSH_HOST = "itds344@10.34.111.33"

    os.system("chmod 700 /opt/airflow/.ssh")
    os.system("chmod 600 /opt/airflow/.ssh/id_rsa")

    # Kafka config (to be adjusted)
    KAFKA_CMD = "/opt/kafka/bin/kafka-console-producer.sh"
    TOPIC = "iot-sensor"

    BATCH_SIZE = 10   # send 10 messages per SSH call
    SLEEP_TIME = 0.2  # simulate streaming

    buffer = []

    with open(file_path, "r") as f:
        next(f)  # skip header

        for line in f:
            row = line.strip().split(",")

            data = {
                "readingID": row[0],
                "timestamp": row[1],
                "roomID": row[2],
                "deviceID": row[3],
                "value": None if row[4] == "" else row[4],
                "deviceType": row[5]
            }

            buffer.append(json.dumps(data))

            # Send batch
            if len(buffer) >= BATCH_SIZE:
                payload = "\\n".join(buffer)

                cmd = f"echo '{payload}' | {KAFKA_CMD} --broker-list localhost:9092 --topic {TOPIC}"

                subprocess.run(
                    ["ssh", SSH_HOST, cmd],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10
                )

                buffer = []
                time.sleep(SLEEP_TIME)

        # Send remaining messages
        if buffer:
            payload = "\\n".join(buffer)

            cmd = f"echo '{payload}' | {KAFKA_CMD} --broker-list localhost:9092 --topic {TOPIC}"

            subprocess.run(
                ["ssh", SSH_HOST, cmd],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10
            )

    print("[INFO] Streaming complete")

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

    task_generate_iot = PythonOperator(
        task_id='generate_iot_data',
        python_callable=generate_iot_data
    )

    # task_stream_kafka = PythonOperator(
    #     task_id='stream_to_kafka',
    #     python_callable=stream_to_kafka
    # )

    # -----------------------
    # PIPELINE ORDER
    # -----------------------
    task_load_timetable >> task_expand_timetable >> task_generate_iot 
    # >> task_stream_kafka