CREATE TABLE R ( A int, B int );
CREATE TABLE S ( B int, C int );

SELECT R.A, S.C FROM R, S WHERE (R.B = S.B) AND ((R.A > S.C) or (R.A < S.C));