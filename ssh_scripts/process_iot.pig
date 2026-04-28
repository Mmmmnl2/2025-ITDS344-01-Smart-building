-- ===========================================================================
-- STEP 1: BRONZE LAYER - LOAD
-- ===========================================================================

-- 1.1 โหลด IoT Data (ไฟล์ที่ Airflow mv มาให้)
raw_iot = LOAD 'file:///home/kanyanat/data/bronze/processing_iot.csv' USING PigStorage(',')
    AS (readingID:chararray, timestamp:chararray, roomID:chararray, deviceID:chararray, value:double, deviceType:chararray);

-- 1.2 โหลด Booking Data (ไฟล์ที่ Airflow mv มาให้)
raw_book = LOAD 'file:///home/kanyanat/data/bronze/processing_booking.csv' USING PigStorage(',')
    AS (b_roomID:chararray, b_date:chararray, b_start:chararray, b_end:chararray, b_id:chararray);

-- 1.3 โหลด Timetable Batch (ตัดคอลัมน์ course ทิ้งไปเลยตั้งแต่ตอนโหลด)
timetable = LOAD 'file:///home/kanyanat/data/bronze/timetable.csv' USING PigStorage(',')
    AS (t_roomID:chararray, t_day:chararray, t_start:chararray, t_end:chararray);

-- ===========================================================================
-- STEP 2: SILVER LAYER - CLEAN & TIME EXTRACT
-- ===========================================================================

-- แยกเวลาจาก timestamp (Format: "18:32:24")
iot_prepared = FOREACH raw_iot GENERATE *, SUBSTRING(timestamp, 11, 19) AS iot_time;

-- กรอง Outlier ตามกฎ 4 ข้อ
cleaned_iot = FILTER iot_prepared BY
    value IS NOT NULL AND (
        (deviceType == 'temperature' AND value < 60) OR
        (deviceType == 'power'       AND value >= 0 AND value < 100) OR
        (deviceType == 'co2'         AND value >= 0 AND value < 2000) OR
        (deviceType == 'humidity'    AND value >= 0 AND value <= 100)
    );

-- ===========================================================================
-- STEP 3: SILVER ENRICHMENT - DECISION ENGINE (The Brain)
-- ===========================================================================

-- JOIN 3 ทางทีละขั้นเพื่อความแม่นยำ
step1 = JOIN cleaned_iot BY roomID LEFT OUTER, raw_book BY b_roomID;
step2 = JOIN step1 BY cleaned_iot::roomID LEFT OUTER, timetable BY t_roomID;

-- 🔹 แก้ไขจุดนี้: ใช้ CASE WHEN เพื่อป้องกันค่าว่าง (Null)
enriched_data = FOREACH step2 GENERATE
    cleaned_iot::readingID AS readingID,
    cleaned_iot::roomID AS roomID,
    cleaned_iot::deviceType AS deviceType,
    cleaned_iot::value AS value,
    (CASE
        WHEN (raw_book::b_start IS NOT NULL AND iot_time >= raw_book::b_start AND iot_time <= raw_book::b_end) THEN 'True'
        WHEN (timetable::t_start IS NOT NULL AND iot_time >= timetable::t_start AND iot_time <= timetable::t_end) THEN 'True'
        ELSE 'False'
     END) AS occupied;

-- Deduplication (ยุบแถวซ้ำจากการ JOIN)
grp_reading = GROUP enriched_data BY (readingID, roomID, deviceType, value);
silver_final = FOREACH grp_reading GENERATE
    group.roomID AS roomID, group.deviceType AS deviceType, group.value AS value,
    MAX(enriched_data.occupied) AS occupied;

STORE silver_final INTO 'file:///home/kanyanat/data/silver_output' USING PigStorage(',');

-- ===========================================================================
-- STEP 4: GOLD LAYER - AGGREGATION
-- ===========================================================================

grp_gold = GROUP silver_final BY (roomID, occupied, deviceType);

gold = FOREACH grp_gold GENERATE
    group.roomID AS roomid,
    group.deviceType AS devicetype,
    group.occupied AS occupied,
    AVG(silver_final.value) AS avg_value,
    MAX(silver_final.value) AS max_value,
    ToString(CurrentTime(), 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ') AS load_date;

STORE gold INTO 'file:///home/kanyanat/data/gold_output' USING PigStorage(',');