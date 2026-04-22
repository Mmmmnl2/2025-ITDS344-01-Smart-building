-- 1. เรียก PiggyBank
REGISTER /usr/local/pig/lib/piggybank.jar;

-- 2. LOAD จาก Local
raw = LOAD 'file:///home/kanyanat/raw_sensor_data.csv' USING PigStorage(',')
      AS (readingID:chararray, timestamp:chararray, roomID:chararray, deviceID:chararray, value:double, deviceType:chararray, occupied:chararray);

-- 3. [SILVER] กรองขยะ (เอาค่า NULL และ Outlier ออก)
cleaned = FILTER raw BY readingID != 'readingID' AND value IS NOT NULL AND value < 400;

-- 4. [GOLD] สรุปผล
grp = GROUP cleaned BY (roomID, deviceType);
gold = FOREACH grp GENERATE
    group.roomID,
    group.deviceType,
    AVG(cleaned.value) AS avg_value,
    MAX(cleaned.value) AS max_value,
    CurrentTime() AS load_date;

-- 5. STORE ลง Local
STORE cleaned INTO 'file:///home/kanyanat/data/silver_output' USING PigStorage(',');
STORE gold INTO 'file:///home/kanyanat/data/gold_output' USING PigStorage(',');