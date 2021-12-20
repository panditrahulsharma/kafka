-- Write your statements to a file

-- Now that you have a series of statements that’s doing the right thing, the last step is to put them into a file so that they can be used outside the CLI session. Create a file at src/statements.sql with the following content:
https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/show-streams/

-- for datetime
https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/scalar-functions/
-- ------------------------------
-- In ksqlDB you can query from the beginning 
SET 'auto.offset.reset' = 'earliest';
--  or end of a topic 
SET 'auto.offset.reset' = 'latest';

-- ---------------------------main sql for project-----------------------------------
-- ===================================================================
-- -----------------drop or create stream-------------------------
"""
tasks
1.replace column data(f,jh,la) concept if then else


raw_telecom >> replace_simple_with_standard_terminology >> combine_all_services
"""

DROP STREAM raw_telecom ;
SET 'auto.offset.reset' = 'earliest';
-- create a raw strem
CREATE STREAM  raw_telecom WITH \
(KAFKA_TOPIC='dbserver2.inventory.raw_telecom', VALUE_FORMAT='AVRO');
-- CREATE STREAM raw_telecom (
--     f VARCHAR,
--     jh VARCHAR,
--     la VARCHAR
--   ) WITH (
--     KAFKA_TOPIC = 'dbserver2.inventory.raw_telecom',
--     VALUE_FORMAT = 'JSON'
--   );

-- 
-- create a stream replace_simple_with_standard_terminology

-- CREATE STREAM replace_simple_with_standard_terminology AS SELECT  AFTER -> A,AFTER -> JH ,AFTER -> LA   FROM RAW_TELECOM;


-- filter of F table
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM replace_simple_with_standard_terminology AS 
SELECT
 CASE
   WHEN AFTER -> F ='Originating' THEN 'Outgoing'
   WHEN AFTER -> F ='Terminating' THEN 'Incoming'
   ELSE AFTER -> F
 END AS F,AFTER -> INDEX,
 CASE
   WHEN AFTER -> JH ='Success' THEN 'Voice Portal'
   ELSE AFTER -> JH
 END AS JH,
 CASE
   WHEN AFTER -> LA ='Shared Call Appearance' THEN 'Secondary Device'
   ELSE AFTER -> LA
 END AS LA,AFTER->ER
FROM RAW_TELECOM
EMIT CHANGES;

-- combine_all_services
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM combine_all_services AS 
SELECT
 CASE
  WHEN  ER IS NULL THEN 
    CASE
      WHEN LA IS NOT NULL and JH IS NOT NULL THEN CONCAT_WS(',',LA,JH)
      WHEN LA IS NOT NULL then LA
    ELSE
      JH
    END
  ELSE  ER
 END AS ER,LA,JH,INDEX
 FROM REPLACE_SIMPLE_WITH_STANDARD_TERMINOLOGY
EMIT CHANGES;

-- -----------------

-- ----------------------------first convert double to bigint-------------------------------------
https://docs.ksqldb.io/en/latest/how-to-guides/use-a-custom-timestamp-column/
-- ----------------------------then bigint to timestamp-----------------------------------------------------------------

-- first step double to bigint
-- change double to varchar
DROP STREAM double_to_BIGINT_timestamp;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM double_to_BIGINT_timestamp
  WITH(KAFKA_TOPIC='double_to_BIGINT_timestamp') AS
  SELECT 
    CAST(AFTER->J AS BIGINT) as start_time
  FROM raw_telecom
  EMIT CHANGES;

-- 2nd step would be bigint to timestamp
https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/scalar-functions/#exp
-- DROP STREAM BIGINT_to_timestamp;
-- SET 'auto.offset.reset' = 'earliest';
-- CREATE STREAM BIGINT_to_timestamp AS 
-- SELECT 
--        CAST(start_time AS VARCHAR) as CAST_date,
--        SUBSTRING(CAST(start_time AS VARCHAR),0,8) as date1,
--        SUBSTRING(CAST(start_time AS VARCHAR),9,15) as date2
-- FROM double_to_BIGINT_timestamp
-- EMIT CHANGES;
DROP STREAM BIGINT_to_timestamp;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM BIGINT_to_timestamp AS 
SELECT 
       CAST(start_time AS VARCHAR) as CAST_date,
       SUBSTRING(CAST(start_time AS VARCHAR),0,8) as call_start_date,
       SUBSTRING(CAST(start_time AS VARCHAR),9,LEN(CAST(start_time AS VARCHAR))) as call_start_time
FROM double_to_BIGINT_timestamp
EMIT CHANGES;
-- -----3rd step--------start date---------------------
DROP STREAM startdate;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM startdate AS 
SELECT 
  CONCAT_WS('-', SUBSTRING(call_start_date,0,4),SUBSTRING(call_start_date,5,2),SUBSTRING(call_start_date,7,2)) as start_call_date
FROM BIGINT_to_timestamp
EMIT CHANGES;

-- -----3rd step--------start time---------------------
-- ---------------------am to pm
DROP STREAM starttime;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM starttime AS 
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
  CONCAT_WS(':',SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2)) as min_sec,
  CONCAT_WS(':', SUBSTRING(CALL_START_TIME,0,2),SUBSTRING(CALL_START_TIME,3,2),SUBSTRING(CALL_START_TIME,5,2)) as start_call_time
FROM BIGINT_to_timestamp
EMIT CHANGES;

-- 4th step am_pm_time
DROP STREAM startdate;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM startdate AS 
SELECT 
  CONCAT_WS('-', SUBSTRING(call_start_date,0,4),SUBSTRING(call_start_date,5,2),SUBSTRING(call_start_date,7,2)) as start_call_date
FROM BIGINT_to_timestamp
EMIT CHANGES;
