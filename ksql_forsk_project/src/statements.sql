-- Write your statements to a file

-- Now that you have a series of statements that’s doing the right thing, the last step is to put them into a file so that they can be used outside the CLI session. Create a file at src/statements.sql with the following content:


-- -----------------------================= main start ===========================
-- ========================================================================================

CREATE STREAM pageviews (
    job VARCHAR,
    company VARCHAR,
    ssn VARCHAR,
    residence VARCHAR,
    username VARCHAR
  ) WITH (
    KAFKA_TOPIC = 'sql-server',
    VALUE_FORMAT = 'JSON'
  );

NOTE: after create pageview go to ksql editor and add following line of code
  """
  select * from PAGEVIEWS EMIT CHANGES;
  """

CREATE STREAM userprofile AS
    SELECT USERNAME,RESIDENCE
      FROM pageviews;

CREATE STREAM userjob AS
    SELECT JOB, SSN,COMPANY
      FROM pageviews;


-- ------------------------------
-- In ksqlDB you can query from the beginning 
SET 'auto.offset.reset' = 'earliest';
--  or end of a topic 
SET 'auto.offset.reset' = 'latest';


-- -----------------drop or create stream-------------------------
DROP STREAM pageviews ;
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM pageviews (
    a VARCHAR,
    b VARCHAR,
    c VARCHAR
  ) WITH (
    KAFKA_TOPIC = 'raw-telecom',
    VALUE_FORMAT = 'JSON'
  );
