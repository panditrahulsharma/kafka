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

CREATE STREAM replace_simple_with_standard_terminology AS SELECT  AFTER -> A,AFTER -> JH ,AFTER -> LA   FROM RAW_TELECOM;


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
