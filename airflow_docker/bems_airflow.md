# IoT streaming data simulation airflow
## Set up

1. Set up ssh connection
ใส่ key ใน `airflow_ssh/id_rsa.pub` เข้าไปใน `~/.ssh/authorized_keys` ใน ssh

2. run `docker compose up airflow-init`
3. run `docker compose down --volumes --rmi all`
4. run `docker compose up -d`
5. open `http://localhost:8080/`

## DAGs
### (basic) generate data from timetable
- `dags/bems_dag.py`
- should work properly
- extract result
    ```
    docker cp [container]:/tmp/sensor_data.csv ./output/sensor_data.csv  
    ```

### (reactive) generate data from timetable AND booking
- `dags/reactive_data_generation.py`
- haven't test with kafka yet
- ในไฟล์ dag: แทนที่ `YOUR_KAFKA_IP` ด้วย ip / หรือ `host.docker.internal`

## Receiving data
In ssh
1. open consumer
    ```
    /opt/kafka/bin/kafka-console-consumer.sh \
        --topic iot-sensor \
        --bootstrap-server localhost:9092
    ``` 
2. test booking
    ```
    ./room_booking.sh 6687089 Napasorn IT101 2026-04-01 12:00 14:00
    ```