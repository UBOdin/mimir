SELECT profit.nation, profit.year, sum(profit.amount) as sum_profit
FROM (
    SELECT nation.name as nation, 
           YEAR(orders.orderdate) as year, 
           lineitem.extendedprice * (1 - lineitem.discount) 
              - partsupp.supplycost * lineitem.quantity AS amount
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE supplier.suppkey = lineitem.suppkey
      AND partsupp.suppkey = lineitem.suppkey
      AND partsupp.partkey = lineitem.partkey
      AND part.partkey = lineitem.partkey
      AND orders.orderkey = lineitem.orderkey
      AND supplier.nationkey = nation.nationkey
      AND part.name LIKE '%green%'
  ) profit
GROUP BY nation, year
ORDER BY nation, year desc;