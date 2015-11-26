SELECT n.name, l.extendedprice * (1 - l.discount) AS revenue 
FROM   xcustomer c, xorders o, xlineitem l, xsupplier s, xnation n, region r
WHERE  c.custkey = o.custkey
  AND  l.orderkey = o.orderkey 
  AND  l.suppkey = s.suppkey
  AND  c.nationkey = s.nationkey 
  AND  s.nationkey = n.nationkey 
  AND  n.regionkey = r.regionkey 
  AND  r.name = 'ASIA'
  AND  o.orderdate >= DATE('1994-01-01')
  AND  o.orderdate <  DATE('1995-01-01');