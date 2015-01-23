SELECT
  lineitem.orderkey,
  sum(extendedprice*(1-discount)) as revenue,
  orders.orderdate,
  orders.shippriority
FROM
  customer, orders, lineitem
WHERE
      customer.mktsegment = 'BUILDING'
  AND customer.custkey = orders.custkey
  AND lineitem.orderkey = orders.orderkey
  AND orders.orderdate < {d '1995-03-15'}
  AND lineitem.shipdate > {d '1995-03-15'}
GROUP BY
  lineitem.orderkey,
  orders.orderdate,
  orders.shippriority
ORDER BY
  revenue desc, orderdate