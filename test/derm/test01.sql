CREATE TABLE DERMATOLOGY_RAW(id number, f1 NUMBER, class_raw char);

create iview dermatology
with missing_value('f1')
as select * from dermatology_raw;


--Analyze condition in
  select id, '1' as class
  from dermatology
  where f1 < 0.5 
union
  select dermatology.*, '2' as class
  from dermatology 
  where f1 > 0.5
