CREATE TABLE DERMATOLOGY_RAW(id number, f1 NUMBER,f2 NUMBER, f3 NUMBER, f4 NUMBER, f5 NUMBER, f6 NUMBER, f7 NUMBER, f8 NUMBER, f9 NUMBER, f10 NUMBER, f11 NUMBER, f12 NUMBER, f13 NUMBER, f14 NUMBER, f15 NUMBER, f16 NUMBER, f17 NUMBER, f18 NUMBER, f19 NUMBER, f20 NUMBER, f21 NUMBER, f22 NUMBER, f23 NUMBER, f24 NUMBER, f25 NUMBER, f26 NUMBER, f27 NUMBER, f28 NUMBER, f29 NUMBER, f30 NUMBER, f31 NUMBER, f32 NUMBER, f33 NUMBER, f34 NUMBER, class_raw char);

create iview dermatology
with missing_value('f1', 'f2', 'f3', 'f4', 'f5', 'f6', 'f7', 'f8', 'f9', 'f10', 'f11', 'f12', 'f13', 'f14', 'f15', 'f16', 'f17', 'f18', 'f19', 'f20', 'f21', 'f22', 'f23', 'f24', 'f25', 'f26', 'f27', 'f28', 'f29', 'f30', 'f31', 'f32', 'f33', 'f34')
as select * from dermatology_raw;


Analyze cond in
  select id, '1' as class
  from dermatology
  where f20 < 0.5 and f27<0.5 and f15<0.5 and f5<0.5 and f7<1.5 and f26<0.5 and f22 >=1.5
union
  select dermatology.*, '2' as class
  from dermatology 
  where f20 < 0.5 and f27<0.5 and f15<0.5 and f5<0.5 and f7<1.5 and f26<0.5 and f22<1.5 and f30 < 2
union
  select dermatology.*, '3' as class
  from dermatology
  where f20<0.5 and f27<0.5 and f15>=0.5 and f5>=1
union
  select dermatology.*, '4' as class
  from dermatology
  where f20<0.5 and f27<0.5 and f15<0.5 and f5<0.5 and f7<1.5 and f26>=0.5
union
  select dermatology.*, '5' as class
  from dermatology
  where f20<0.5 and f27<0.5 and f15>=0.5 and f5<1
union
  select dermatology.*, '6' as class
  from dermatology
  where f20 < 0.5 and f27<0.5 and f15<0.5 and f5<0.5 and f7<1.5 and f26<0.5 and f22<1.5 and f30 >=2

-- Should output a list of variables ranked by CPI