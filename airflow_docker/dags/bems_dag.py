import logging
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.utils.task_group import TaskGroup
from textwrap import dedent

# --- Configuration ---
BASE_PATH = "/opt/airflow/data"
BRONZE_PATH = f"{BASE_PATH}/bronze"
REMOTE_SCRIPTS = "/home/kanyanat/scripts"
REMOTE_DATA = "/home/kanyanat/data"

default_args = {
    'owner': 'smart-building',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'bems_medallion_pipeline_v4',
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval=timedelta(minutes=5),
    catchup=False
) as dag:

    with TaskGroup("bronze_layer") as bronze:
        t_transfer = SFTPOperator(
            task_id="sync_streaming_file",
            ssh_conn_id="linux_server_connection",
            local_filepath=f"{BRONZE_PATH}/raw_sensor_data.csv",
            remote_filepath="/home/kanyanat/raw_sensor_data.csv",
            operation="put"
        )

    with TaskGroup("processing") as processing:
        t_pig_etl = SSHOperator(
            task_id="run_pig_and_archive",
            ssh_conn_id="linux_server_connection",
            command=dedent(f"""
                    export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
                    export HADOOP_HOME=/usr/local/hadoop
                    export PIG_HOME=/usr/local/pig
                    export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$PIG_HOME/bin

                    # 🔹 1. เช็คก่อนว่าไฟล์มาถึง Linux จริงไหม
                    if [ ! -f /home/kanyanat/raw_sensor_data.csv ]; then
                        echo "❌ ERROR: ข้อมูลดิบยังไม่มาถึง!"
                        exit 1
                    fi

                    # 🔹 2. ล้าง Folder Output เก่า
                    rm -rf /home/kanyanat/data/silver_output /home/kanyanat/data/gold_output

                    # 🔹 3. รัน Pig
                    $PIG_HOME/bin/pig -x local /home/kanyanat/scripts/process_iot.pig

                    # เช็คว่า Pig รันสำเร็จไหมก่อนไปต่อ
                    if [ $? -eq 0 ]; then
                        # 🔹 4. เอาข้อมูลดิบไปต่อท้ายใน Data Lake (HDFS)
                        $HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/kanyanat/bronze/
                        $HADOOP_HOME/bin/hdfs dfs -appendToFile /home/kanyanat/raw_sensor_data.csv /user/kanyanat/bronze/all_history_data.csv

                        # 🔹 5. ใช้เสร็จแล้วลบไฟล์ทิ้ง (เฉพาะไฟล์บน Linux)
                        rm -f /home/kanyanat/raw_sensor_data.csv
                        echo "✅ ประมวลผลและเก็บเข้า Data Lake สำเร็จ!"
                    else
                        echo "❌ Pig รันไม่ผ่าน เช็ค Log ใน pig_*.log"
                        exit 1
                    fi
                """),
        )

    with TaskGroup("serving_layer_mongo") as serving:
        t_mongo_load = SSHOperator(
            task_id="load_to_mongodb",
            ssh_conn_id="linux_server_connection",
            command=dedent(f"""
                mongoimport --db smart_building \
                            --collection fact_iot_readings \
                            --type csv \
                            --fields roomid,devicetype,avg_value,max_value,load_date \
                            --file {REMOTE_DATA}/gold_output/part-r-00000 \
                            --mode merge \
                            --upsertFields roomid,devicetype
            """),
        )

    bronze >> processing >> serving