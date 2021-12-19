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
 END AS LA
FROM RAW_TELECOM
EMIT CHANGES;

-- combine_all_services
CREATE STREAM combine_all_services AS 
SELECT
 CASE
  WHEN AFTER -> ER IS NULL THEN 
    CASE
      WHEN AFTER->LA IS NOT NULL and AFTER->JH IS NOT NULL THEN CONCAT_WS(',',AFTER->LA,AFTER->JH)
      WHEN AFTER->LA IS NOT NULL then AFTER->LA
    ELSE
      AFTER->JH
    END
  ELSE AFTER -> ER
 END AS ER