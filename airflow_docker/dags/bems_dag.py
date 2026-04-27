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
    'bems_medallion_pipeline_v6',
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
                # 1. เช็คว่ามีไฟล์ gold_output ออกมาจริงไหม
                if ls /home/kanyanat/data/gold_output/part-* 1> /dev/null 2>&1; then

                    # 2. Import ข้อมูลรอบใหม่เข้า MongoDB (ใช้โหมดปกติเพื่อสะสมประวัติ)
                    mongoimport --db smart_building \
                               --collection fact_iot_readings \
                               --type csv \
                               --fields roomid,devicetype,occupied,avg_value,max_value,load_date \
                               --file $(ls /home/kanyanat/data/gold_output/part-*)

                    # 3. รัน Data Retention Policy (เก็บแค่ 3 load_date ล่าสุด)
                    mongosh smart_building --eval '
                        const latestDates = db.fact_iot_readings.distinct("load_date").sort().reverse().slice(0, 3);
                        const result = db.fact_iot_readings.deleteMany({{ load_date: {{ $nin: latestDates }} }});
                        print("✅ Retention Complete: Deleted " + result.deletedCount + " old records.");
                    '
                else
                    echo "❌ ERROR: ไม่พบไฟล์ Gold Output ใน /home/kanyanat/data/gold_output/"
                    exit 1
                fi
            """),
        )

    with TaskGroup("testing") as testing:
        t_validate_e2e = SSHOperator(
            task_id="validate_e2e_results",
            ssh_conn_id="linux_server_connection",
            command=dedent("""
                echo "🚀 Starting Comprehensive End-to-End Validation..."
                DB_NAME="smart_building"
                COLLECTION="fact_iot_readings"

                # --- 1. Integrity Check ---
                HEADER_COUNT=$(mongosh $DB_NAME --quiet --eval "db.$COLLECTION.countDocuments({ roomid: 'readingID' })")

                NULL_COUNT=$(mongosh $DB_NAME --quiet --eval "db.$COLLECTION.countDocuments({
                    \$or: [
                        { roomid: null },
                        { devicetype: null },
                        { avg_value: null }
                    ]
                })")

                # --- 2. Logic Check ---
                OUTLIER_LEAK=$(mongosh $DB_NAME --quiet --eval "db.$COLLECTION.countDocuments({
                    \$or: [
                        { devicetype: 'temperature', avg_value: { \$gt: 60 } },
                        { devicetype: 'power', avg_value: { \$gte: 100 } },
                        { devicetype: 'power', avg_value: { \$lt: 0 } },
                        { devicetype: 'co2', avg_value: { \$gte: 2000 } },
                        { devicetype: 'humidity', avg_value: { \$gt: 100 } }
                    ]
                })")

                # --- 3. สรุปผล (ลบ \ หน้าตัวแปรออกให้หมด) ---
                TOTAL_ERRORS=$((HEADER_COUNT + NULL_COUNT + OUTLIER_LEAK))

                echo "📊 Validation Summary:"
                echo "   - Header Records Found: $HEADER_COUNT"
                echo "   - Null Records Found:   $NULL_COUNT"
                echo "   - Outlier Records Found: $OUTLIER_LEAK"

                if [ "$TOTAL_ERRORS" -eq 0 ]; then
                    echo "✅ SUCCESS: All E2E Data Quality Rules Passed!"
                else
                    echo "❌ FAIL: Data Integrity Issues Detected! (Total Errors: $TOTAL_ERRORS)"
                    exit 1
                fi
            """),
        )

    bronze >> processing >> serving >> testing