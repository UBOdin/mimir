SELECT n.name, l.extendedprice * (1 - l.discount) AS revenue 
FROM   xLARGEcustomer c, xLARGEorders o, xLARGElineitem l, xLARGEsupplier s, xLARGEnation n, LARGEregion r
WHERE  c.custkey = o.custkey
  AND  l.orderkey = o.orderkey 
  AND  l.suppkey = s.suppkey
  AND  c.nationkey = s.nationkey 
  AND  s.nationkey = n.nationkey 
  AND  n.regionkey = r.regionkey 
  AND  r.name = 'ASIA'
  AND  o.orderdate >= TO_DATE('1994-01-01', 'YYYY-MM-DD')
  AND  o.orderdate <  TO_DATE('1995-01-01', 'YYYY-MM-DD');