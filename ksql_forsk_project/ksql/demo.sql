-- time diff
-- https://stackoverflow.com/questions/67774680/get-timestamp-difference-in-ksql

-- https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-43-timestamp-data-type-support.md

-- convert a date and time to timestamp format
SELECT CAST(CONCAT_WS('T',START_CALL_DATE,START_CALL_TIME) AS TIMESTAMP) AS last_switched
FROM DATE_TIME_COLUMN EMIT CHANGES;

-- ----------------------------------------

SELECT CONCAT_WS('T',END_CALL_DATE,END_CALL_TIME) as start_time,
CONCAT_WS('T',START_CALL_DATE,START_CALL_TIME) as end_time,
(UNIX_TIMESTAMP(CAST(CONCAT_WS('T',END_CALL_DATE,END_CALL_TIME) AS TIMESTAMP))-UNIX_TIMESTAMP(CAST(CONCAT_WS('T',START_CALL_DATE,START_CALL_TIME) AS TIMESTAMP)))/1000 AS duration
FROM DATE_TIME_COLUMN EMIT CHANGES;


-- --------------------\

-- -----3rd step--------start date with week day name---------------------
DROP STREAM if exists date_time_column;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM date_time_column AS 
SELECT 
  (UNIX_TIMESTAMP(CAST(CONCAT_WS('T',call_end_date,call_end_time) AS TIMESTAMP))-UNIX_TIMESTAMP(CAST(CONCAT_WS('T',call_start_date,CALL_START_TIME) AS TIMESTAMP)))/1000 AS duration
FROM BIGINT_to_timestamp
EMIT CHANGES;

-- get time diff using ksql
SELECT CONCAT_WS('T',END_CALL_DATE,END_CALL_TIME) as start_time,
CONCAT_WS('T',START_CALL_DATE,START_CALL_TIME) as end_time,
(UNIX_TIMESTAMP(CAST(CONCAT_WS('T',END_CALL_DATE,END_CALL_TIME) AS TIMESTAMP))-UNIX_TIMESTAMP(CAST(CONCAT_WS('T',START_CALL_DATE,START_CALL_TIME) AS TIMESTAMP)))/1000 AS duration
FROM DATE_TIME_COLUMN EMIT CHANGES;




-- ----------------
DROP STREAM if exists date_time_column;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM date_time_column AS 
SELECT 
  CASE 
    WHEN CAST(SUBSTRING(CALL_START_TIME,0,2) as int)>=12 THEN
      CASE  
        WHEN  CAST(SUBSTRING(CALL_START_TIME,0,2) as int)=12 THEN  CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2)) 
      ELSE
        CONCAT_WS(':', CAST(CAST(SUBSTRING(CALL_START_TIME,0,2) as int)-12 AS VARCHAR),SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2))
      END
    ELSE
      CASE
        WHEN CAST(SUBSTRING(CALL_START_TIME,0,2) as int) =0 then CONCAT_WS(':','12',SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2))
      ELSE
        CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2))
      END  
  END as am_pm_time,
  CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2)) as start_call_time,
  CONCAT_WS('-', SUBSTRING(call_start_date,0,4),SUBSTRING(call_start_date,5,2),SUBSTRING(call_start_date,7,2)) as start_call_date,
  CONCAT_WS(':', SUBSTRING(call_end_time,0,2),SUBSTRING(call_end_time,3,2),SUBSTRING(call_end_time,5,2)) as end_call_time,
  CONCAT_WS('-', SUBSTRING(call_end_date,0,4),SUBSTRING(call_end_date,5,2),SUBSTRING(call_end_date,7,2)) as end_call_date,
  FORMAT_DATE(CAST(CONCAT_WS('-', SUBSTRING(call_start_date,0,4),SUBSTRING(call_start_date,5,2),SUBSTRING(call_start_date,7,2)) as date), 'E') as weekly_range,
  CONCAT_WS('-',CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),'00'),CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),'59')) as hourly_range,
  (UNIX_TIMESTAMP(CAST(CONCAT_WS('T',
      CONCAT_WS('-', SUBSTRING(call_end_date,0,4),SUBSTRING(call_end_date,5,2),SUBSTRING(call_end_date,7,2)),
      CONCAT_WS(':', SUBSTRING(call_end_time,0,2),SUBSTRING(call_end_time,3,2),SUBSTRING(call_end_time,5,2))) 
      AS TIMESTAMP))-
  UNIX_TIMESTAMP(CAST(CONCAT_WS('T',
    CONCAT_WS('-', SUBSTRING(call_start_date,0,4),SUBSTRING(call_start_date,5,2),SUBSTRING(call_start_date,7,2)),
    CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2))) AS TIMESTAMP))
    )/1000 AS duration

FROM BIGINT_to_timestamp
EMIT CHANGES;
