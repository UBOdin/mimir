SELECT ORDERS.orderkey, 
       ORDERS.orderdate,
       ORDERS.shippriority,
       extendedprice * (1 - discount) AS query3
FROM   CUSTOMER, ORDERS, LINEITEM
WHERE  CUSTOMER.mktsegment = 'BUILDING'
  AND  ORDERS.custkey = CUSTOMER.custkey
  AND  LINEITEM.orderkey = ORDERS.orderkey
  AND  ORDERS.orderdate < DATE('1995-03-15')
  AND  LINEITEM.shipdate > DATE('1995-03-15');

