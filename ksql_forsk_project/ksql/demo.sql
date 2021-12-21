-- time diff
-- https://stackoverflow.com/questions/67774680/get-timestamp-difference-in-ksql

-- https://github.com/confluentinc/ksql/blob/master/design-proposals/klip-43-timestamp-data-type-support.md

-- convert a date and time to timestamp format
SELECT CAST(CONCAT_WS('T',START_CALL_DATE,START_CALL_TIME) AS TIMESTAMP) AS last_switched
FROM DATE_TIME_COLUMN EMIT CHANGES;
