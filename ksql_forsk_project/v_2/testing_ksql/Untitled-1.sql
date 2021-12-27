CREATE STREAM  address_stream WITH \
(KAFKA_TOPIC='dbserver2.inventory.raw_address', VALUE_FORMAT='AVRO');
as AFTER -> JOB

PRINT ADDRESS_STREAM FROM BEGINNING LIMIT 2;


DROP STREAM if exists job_stream;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM job_stream WITH (VALUE_FORMAT = 'JSON') AS 
select AFTER -> JOB , AFTER -> COMPANY
from ADDRESS_STREAM EMIT CHANGES;

DROP STREAM if exists job_stream2;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM job_stream2 WITH (VALUE_FORMAT = 'JSON') AS 
select JOB,COMPANY
from job_stream1 EMIT CHANGES;

