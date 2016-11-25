CREATE TABLE R (A int, B int);
CREATE TABLE S (B int, C int);
CREATE TABLE T (C int, D int);

SELECT A, SUM(D) FROM R, S, T WHERE (R.B = S.B) AND (S.C = T.C) GROUP BY A;