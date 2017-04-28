-- CREATE LENS cust_q1 AS
-- SELECT cm.tid,
--        cm.mktsegment,
--        cc.custkey
-- FROM cust_c_mktsegment cm,
--      cust_c_custkey cc
-- WHERE cm.tid = cc.tid
-- WITH key_repair(tid);

-- CREATE LENS orders_q1 AS
-- SELECT ok.tid,
--        ok.orderkey,
--        od.orderdate,
--        os.shippriority,
--        ock.custkey
-- FROM orders_o_orderkey ok,
--      orders_o_orderdate od,
--      orders_o_shippriority os,
--      orders_o_custkey ock
-- WHERE ok.tid = od.tid
--   AND od.tid = os.tid
--   AND os.tid = ock.tid
-- WITH key_repair(tid);

-- CREATE LENS lineitem_q1 AS
-- SELECT lok.tid,
--        lok.orderkey,
--        ls.shipdate
-- FROM lineitem_l_orderkey lok,
--      lineitem_l_shipdate ls
-- WHERE lok.tid = ls.tid
-- WITH key_repair(tid);

CREATE TABLE res_q1_mimir AS
SELECT o.orderkey,
       o.orderdate,
       o.shippriority
FROM cust_q1 c,
     orders_q1 o,
     lineitem_q1 l
WHERE o.orderdate > DATE('1995-03-15')
  AND l.shipdate < DATE('1995-03-17')
  AND c.mktsegment = 'BUILDING'
  AND c.custkey = o.custkey
  AND o.orderkey = l.orderkey;

