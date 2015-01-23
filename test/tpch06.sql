SELECT SUM(l.extendedprice, l.discount) as revenue
FROM lineitem l
WHERE l.shipdate >= {d '1994-01-01'}
  AND l.shipdate < {d '1995-01-01'}
  AND l.discount between 0.05 AND 0.07
  AND l.quantity < 24