CREATE STREAM TEMPERATURE_READINGS_TIMESTAMP_MT AS
SELECT TEMPERATURE, CONVERT_TZ(FROM_UNIXTIME(EVENTTIME), 'UTC', 'America/Denver') AS EVENTTIME_MT
FROM TEMPERATURE_READINGS_RAW;


CREATE STREAM raw_time (
    AFTER -> J VARCHAR,
    AFTER -> N VARCHAR
  ) WITH (
    KAFKA_TOPIC = 'dbserver2.inventory.raw_telecom',
    VALUE_FORMAT = 'AVRO'
  );


-- change double to varchar
DROP STREAM double_to_var_timestamp;
CREATE STREAM double_to_var_timestamp
  WITH(KAFKA_TOPIC='double_to_var_timestamp') AS
  SELECT 
    CAST(AFTER->J AS VARCHAR) as start_time
  FROM raw_telecom
  EMIT CHANGES;

-- change varchar to timestamp

DROP STREAM var_to_timestamp;
CREATE STREAM var_to_timestamp
  WITH(KAFKA_TOPIC='var_to_timestamp',timestamp='start_time',timestamp_format='yyyy.MM.dd G ''at'' HH:mm:ss z') AS
  SELECT 
    start_time 
  FROM double_to_var_timestamp
  EMIT CHANGES;


CREATE STREAM TEMPERATURE_READINGS_TIMESTAMP_MT AS
SELECT STRINGTOTIMESTAMP(start_time, 'yyyy-MM-dd HH') AS EVENTTIME_MT
FROM double_to_var_timestamp;


{"START_TIME":"2.019062018355677E13"}

STRINGTODATE('2.019062018355677E13', 'yyyy-MM-dd')
