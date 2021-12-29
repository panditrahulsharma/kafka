   datatable_data = call_analytics_data.groupby(['Group','UserID','UserDeviceType'])['Call_Direction'].value_counts().unstack(fill_value=0).reset_index() 
        

SELECT * FROM table
GROUP BY col1, col2

-- select group,user_id,Device_type,Call_Direction
select e,dq,mh,f from raw_telecom;
mysql> select e,dq,mh,f from raw_telecom where mh is not null;

-- group by group,user_id,Device_type

SELECT e,dq,mh,
sum(case when f = 'Originating' then 1 else 0 end) as Originating_call,
sum(case when f = 'Terminating' then 1 else 0 end) as Terminating_call
 FROM raw_telecom GROUP BY e,dq,mh;


CREATE TABLE TABLE_DATA AS
    SELECT GROUPS,USER_ID,USERDEVICETYPE,
    sum(case when CALL_DIRECTION = 'Incoming' then 1 else 0 end) as INCOMINF,
    sum(case when CALL_DIRECTION = 'Outgoing' then 1 else 0 end) as OUTGOING,
    sum(DATE_TIME_COLUMN_DURATION) Total_CALL_DURATION
    FROM CALL_DATASETS 
    GROUP BY GROUPS,USER_ID,USERDEVICETYPE 
    EMIT CHANGES;




DROP STREAM if exists TABLE_DATA;
SET 'auto.offset.reset' = 'earliest';
CREATE TABLE TABLE_DATA AS
    SELECT GROUPS,USER_ID,USERDEVICETYPE,
    sum(DATE_TIME_COLUMN_DURATION) Total_CALL_DURATION
    FROM CALL_DATASETS WINDOW TUMBLING (size 10 seconds)
    GROUP BY GROUPS,USER_ID,USERDEVICETYPE 
    EMIT CHANGES;


  SELECT GROUPS,USER_ID,USERDEVICETYPE,
  sum(case when CALL_DIRECTION = 'Incoming' then 1 else 0 end) as INCOMING,
  sum(case when CALL_DIRECTION = 'Outgoing' then 1 else 0 end) as OUTGOING,
  sum(DATE_TIME_COLUMN_DURATION) Total_CALL_DURATION
  FROM CALL_DATASETS
  GROUP BY GROUPS,USER_ID,USERDEVICETYPE 
  EMIT CHANGES;