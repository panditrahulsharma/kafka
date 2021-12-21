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
