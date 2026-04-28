-- 1. LOAD ข้อมูลเซนเซอร์ (Fact)
raw_sensor = LOAD 'file:///home/kanyanat/raw_sensor_data.csv' USING PigStorage(',')
      AS (readingID:chararray, timestamp:chararray, roomID:chararray, deviceID:chararray, value:double, deviceType:chararray, occupied:chararray);

-- 2. [SILVER] กรองขยะเบื้องต้น
cleaned_sensor = FILTER raw_sensor BY
    readingID != 'readingID' AND
    value IS NOT NULL AND
    (
        (deviceType == 'temperature' AND value < 60) OR
        (deviceType == 'power' AND value >= 0 AND value < 100) OR
        (deviceType == 'co2' AND value >= 0 AND value < 2000) OR
        (deviceType == 'humidity' AND value >= 0 AND value <= 100)
    );

-- เก็บข้อมูลชั้น Silver (ข้อมูลที่สะอาดแล้วแต่ยังไม่ถูกเติมบริบท)
STORE cleaned_sensor INTO 'file:///home/kanyanat/data/silver_output' USING PigStorage(',');

-- 3. LOAD ข้อมูลตารางเรียน (Reference Data)
timetable = LOAD 'file:///home/kanyanat/timetable.csv' USING PigStorage(',')
      AS (t_roomID:chararray, day:chararray, t_start:chararray, t_end:chararray);

-- 4. [DATA ENRICHMENT] ทำการ JOIN เพื่อระบุสถานะการใช้งาน
-- ใช้ Inner Join: ข้อมูลที่หลุดออกมาจากขั้นตอนนี้คือข้อมูลที่มีตารางเรียนรองรับเท่านั้น
joined_data = JOIN cleaned_sensor BY roomID, timetable BY t_roomID;

-- 🔹 เปลี่ยนสถานะ occupied เป็น 'True' และดึงชื่อวิชา (subject) มาแสดง
enriched_data = FOREACH joined_data GENERATE
    cleaned_sensor::roomID AS roomID,
    cleaned_sensor::deviceType AS deviceType,
    cleaned_sensor::value AS value,
    'True' AS occupied;

-- 5. [GOLD LAYER] จัดกลุ่มและคำนวณสถิติ
-- รวมกลุ่มตามห้อง ประเภทอุปกรณ์ สถานะ occupied และชื่อวิชา
grp = GROUP enriched_data BY (roomID, deviceType, occupied);

gold = FOREACH grp GENERATE
    group.roomID,
    group.deviceType,
    group.occupied,
    AVG(enriched_data.value) AS avg_value,
    MAX(enriched_data.value) AS max_value,
    CurrentTime() AS load_date;

-- 6. STORE Gold Layer (เพื่อส่งไป MongoDB)
STORE gold INTO 'file:///home/kanyanat/data/gold_output' USING PigStorage(',');