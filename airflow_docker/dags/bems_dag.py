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
        t_transfer_timetable = SFTPOperator(
            task_id="sync_timetable_file",
            ssh_conn_id="linux_server_connection",
            local_filepath=f"{BASE_PATH}/timetable.csv",
            remote_filepath="/home/kanyanat/timetable.csv",
            operation="put"
        )

        t_transfer = SFTPOperator(
            task_id="sync_streaming_file",
            ssh_conn_id="linux_server_connection",
            local_filepath=f"{BRONZE_PATH}/raw_sensor_data.csv",
            remote_filepath="/home/kanyanat/raw_sensor_data.csv",
            operation="put"
        )

        t_transfer_timetable >> t_transfer

    with TaskGroup("processing") as processing:
        t_pig_etl = SSHOperator(
            task_id="run_pig_and_archive",
            ssh_conn_id="linux_server_connection",
            command=dedent(f"""
                export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
                export HADOOP_HOME=/usr/local/hadoop
                export PIG_HOME=/usr/local/pig
                export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$PIG_HOME/bin

                export COMMONS_JAR="/usr/local/pig/lib/hadoop3-runtime/commons-collections-3.2.2.jar"
                export PIG_CLASSPATH=$PIG_CLASSPATH:$COMMONS_JAR

                if [ ! -f /home/kanyanat/raw_sensor_data.csv ] || [ ! -f /home/kanyanat/timetable.csv ]; then
                    echo "❌ ERROR: ไฟล์ Input หายไป!"
                    exit 1
                fi

                rm -rf /home/kanyanat/data/silver_output /home/kanyanat/data/gold_output

                rm -f pig_*.log

                # ลบ flag -file และเพิ่มการพิมพ์ Log Pig ให้ออกจอ
                $PIG_HOME/bin/pig -x local -f /home/kanyanat/scripts/process_iot.pig

                if [ $? -eq 0 ]; then
                    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/kanyanat/bronze/
                    $HADOOP_HOME/bin/hdfs dfs -appendToFile /home/kanyanat/raw_sensor_data.csv /user/kanyanat/bronze/all_history_data.csv
                    rm -f /home/kanyanat/raw_sensor_data.csv
                    echo "✅ ประมวลผลสำเร็จ!"
                else
                    echo "❌ Pig รันไม่ผ่าน! นี่คือ Log ล่าสุด:"
                    cat pig_*.log || echo "ไม่พบไฟล์ log"
                    exit 1
                fi
            """)
        )

    with TaskGroup("serving_layer_mongo") as serving:
        t_mongo_load = SSHOperator(
            task_id="load_to_mongodb",
            ssh_conn_id="linux_server_connection",
            command=dedent(f"""
                # เช็คว่ามีไฟล์ gold_output ออกมาจริงไหมก่อน Import
                if [ -d /home/kanyanat/data/gold_output ]; then
                    mongoimport --db smart_building \
                                --collection fact_iot_readings \
                                --type csv \
                                --fields roomid,devicetype,avg_value,max_value,load_date \
                                --file $(ls /home/kanyanat/data/gold_output/part-*) \
                                --mode merge \
                                --upsertFields roomid,devicetype
                else
                    echo "❌ ERROR: ไม่พบไฟล์ Gold Output สำหรับ Import เข้า MongoDB"
                    exit 1
                fi
            """),
        )

    bronze >> processing >> serving