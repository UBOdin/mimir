SELECT supplier.acctbal
FROM supplier, nation, 
  (SELECT DISTINCT partsupp.suppkey
     FROM (SELECT DISTINCT part.partkey FROM part WHERE part.name LIKE 'forest%') p,
          (SELECT lineitem.partkey, lineitem.suppkey, 0.5 * sum(lineitem.quantity) as qty
             FROM lineitem
            WHERE lineitem.shipdate >= {d '1994-01-01'}
              AND lineitem.shipdate < {d '1995-01-01'}
           GROUP BY lineitem.partkey, lineitem.suppkey
          ) l,
          partsupp
    WHERE p.partkey = partsupp.partkey
      AND l.partkey = partsupp.partkey
      AND l.suppkey = partsupp.suppkey
      AND l.qty < partsupp.availqty
  ) ps
WHERE supplier.suppkey = ps.suppkey
  AND supplier.nationkey = nation.nationkey
  AND nation.name = 'CANADA'
ORDER BY supplier.name
