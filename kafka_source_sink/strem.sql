-- inside ksql
-- create stream

CREATE STREAM pageviews (viewtime BIGINT, job VARCHAR, mail VARCHAR)
  WITH (VALUE_FORMAT = 'JSON',
        KAFKA_TOPIC = 'dbserver2.inventory.USERS');

-- 
DESCRIBE EXTENDED pageviews;