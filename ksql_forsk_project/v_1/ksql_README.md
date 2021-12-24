## imp links(ksql)
    https://kafka-tutorials.confluent.io/split-a-stream-of-events-into-substreams/ksql.html

## destroy confluent local data
    confluent local destroy

## timestamp
```
CREATE STREAM pageviews_transformed
  WITH (TIMESTAMP='viewtime',
        PARTITIONS=5,
        VALUE_FORMAT='JSON') AS
  SELECT viewtime,
         userid,
         pageid,
         TIMESTAMPTOSTRING(viewtime, 'yyyy-MM-dd HH:mm:ss.SSS') AS timestring
  FROM pageviews
  PARTITION BY userid
  EMIT CHANGES;
```