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

### 3. วิธีเปิด dashboard (streamlit)
**วิธี setup สามารถดูได้ใน:**
**ใน ssh ของกลุ่ม**
1. รันเว็บไซต์ใน ssh
  ```
  cd ~/Smart-building/dashboard
  source venv/bin/activate
  
  streamlit run your_app.py --server.address 127.0.0.1
  ```
2. เปิด Tunnel ssh เพื่อเชื่อมต่อ (แก้ปัญหา tailscale) **รันใน terminal ใหม่**
  ```
  ssh -L 8501:localhost:8501 kanyanat@100.110.59.93
  ```
3. เข้าถึงผ่าน `http://localhost:8501/`
