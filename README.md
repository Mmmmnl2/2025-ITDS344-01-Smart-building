# 2025-ITDS344-01-Smart-building

Project flow
| Steps | Tools | Files |
|-------|-------|-------|
| Data Simulation & Ingestion | Airflow + Kafka | [airflow_docker.md](airflow_docker/bems_airflow.md)

## How to Run

### 1. วิธีเปิดการสร้างข้อมูล iot-sensor (streaming)
เข้าไปที่ shell ของ Airflow webserver container:
```bash
docker exec -it airflow_docker-airflow-webserver-1 bash
```
จากนั้นรันคำสั่ง:
```bash
python /opt/airflow/dags/reactive_iot_streamer.py
```

### 2. วิธีเปิดการสร้างข้อมูล room-booking (Kafka Event)
เข้าไปที่ shell ของ Airflow webserver container:
```bash
docker exec -it airflow_docker-airflow-webserver-1 bash
```
จากนั้นรันคำสั่ง:
```bash
python /opt/airflow/dags/booking_producer.py
```
