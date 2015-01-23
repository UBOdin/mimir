CREATE TABLE R(A int, B int);

CREATE VIEW V AS 
  SELECT (case when R.A = null then Var('A', R.ROWID)
         else R.A end) AS A, B
  FROM R;

SELECT B FROM V WHERE V.A < 3;