CREATE TABLE R(A int, B int);
CREATE TABLE S(A int, B int);

CREATE VIEW T AS
  SELECT * 
  FROM 
    (SELECT A, B, 
            VAR('X', R.B) AS N 
     FROM R) Q, 
    S 
  WHERE Q.N = S.B;

SELECT A, N FROM T;

