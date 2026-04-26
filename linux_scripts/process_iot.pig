-- 1. LOAD ข้อมูลเซนเซอร์ (Fact)
raw_sensor = LOAD 'file:///home/kanyanat/raw_sensor_data.csv' USING PigStorage(',')
      AS (readingID:chararray, timestamp:chararray, roomID:chararray, deviceID:chararray, value:double, deviceType:chararray, occupied:chararray);

-- 2. [SILVER] กรองขยะเบื้องต้น (Header, NULL, Outlier)
cleaned_sensor = FILTER raw_sensor BY
    readingID != 'readingID' AND
    value IS NOT NULL AND
    (
        (deviceType == 'temperature' AND value < 60) OR   -- อุณหภูมิห้ามเกิน 60 (เผื่อเซนเซอร์ตากแดด)
        (deviceType == 'power' AND value < 400) OR       -- พลังงานห้ามเกิน 400
        (deviceType == 'co2' AND value < 2000) OR        -- CO2 ห้ามเกิน 2000 ppm
        (deviceType == 'humidity' AND value <= 100)      -- ความชื้นห้ามเกิน 100%
    );

-- 🔹 เพิ่มจุดนี้: STORE Silver Layer ไว้ใช้งานและทำ Report
-- แนะนำให้ล้าง Folder เก่าใน Airflow ก่อนรันด้วยนะ
STORE cleaned_sensor INTO 'file:///home/kanyanat/data/silver_output' USING PigStorage(',');

-- 3. LOAD ข้อมูลตารางเรียน (Reference Data)
timetable = LOAD 'file:///home/kanyanat/timetable.csv' USING PigStorage(',')
      AS (t_roomID:chararray, subject:chararray, day:chararray, t_start:chararray, t_end:chararray);

-- 4. [DATA ENRICHMENT & GOLD]
enriched_data = JOIN cleaned_sensor BY roomID, timetable BY t_roomID;
grp = GROUP enriched_data BY (cleaned_sensor::roomID, cleaned_sensor::deviceType);

gold = FOREACH grp GENERATE
    group.roomID,
    group.deviceType,
    AVG(enriched_data.value) AS avg_value,
    MAX(enriched_data.value) AS max_value,
    CurrentTime() AS load_date;

-- 5. STORE Gold Layer (เพื่อส่งไป MongoDB)
STORE gold INTO 'file:///home/kanyanat/data/gold_output' USING PigStorage(',');