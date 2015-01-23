CREATE TABLE R(A int, B int);
CREATE TABLE S(A int, B int);


SELECT * 
FROM 
  (SELECT A, B, 
          TABLE_VAR('X', 'UNIFORM') AS N 
   FROM R
  ) Q, 
  S 
WHERE Q.B = S.B AND Q.N = S.C

