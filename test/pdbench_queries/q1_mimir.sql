CREATE TABLE res_q1_mimir AS
SELECT l_orderkey,
       o_orderdate,
       o_shippriority
FROM
  (SELECT l_orderkey,
          o_orderdate,
          o_shippriority,
          lineitem_tid,
          customer_tid,
          orders_tid
   FROM
     (SELECT lineitem_tid,
             customer_tid,
             orders_tid,
             l_orderkey,
             o_orderdate
      FROM
        (SELECT l_orderkey,
                o_orderdate,
                o_orderkey,
                lineitem_tid,
                customer_tid,
                orders_tid
         FROM
           (SELECT l_orderkey,
                   lineitem_tid
            FROM
              (SELECT tid AS lineitem_tid,
                      orderkey AS l_orderkey
               FROM lineitem_l_orderkey_run_1) AS U1
            JOIN
              (SELECT lineitem_tid
               FROM
                 (SELECT *
                  FROM
                    (SELECT tid AS lineitem_tid,
                            shipdate AS l_shipdate
                     FROM lineitem_l_shipdate_run_1) AS X1
                  WHERE l_shipdate < '1995-03-17') U) AS U2 USING (lineitem_tid)) AS U1,

           (SELECT o_orderdate,
                   o_orderkey,
                   customer_tid,
                   orders_tid
            FROM
              (SELECT customer_tid,
                      orders_tid,
                      o_orderdate
               FROM
                 (SELECT c_custkey,
                         o_custkey,
                         o_orderdate,
                         customer_tid,
                         orders_tid
                  FROM
                    (SELECT c_custkey,
                            customer_tid
                     FROM
                       (SELECT tid AS customer_tid,
                               custkey AS c_custkey
                        FROM cust_c_custkey_run_1) AS U1
                     JOIN
                       (SELECT customer_tid
                        FROM
                          (SELECT *
                           FROM
                             (SELECT tid AS customer_tid,
                                     mktsegment AS c_mktsegment
                              FROM cust_c_mktsegment_run_1) AS X2
                           WHERE c_mktsegment = 'BUILDING') U) AS U2 USING (customer_tid)) AS U1,

                    (SELECT o_custkey,
                            o_orderdate,
                            orders_tid
                     FROM
                       (SELECT tid AS orders_tid,
                               custkey AS o_custkey
                        FROM orders_o_custkey_run_1) AS U1
                     JOIN
                       (SELECT *
                        FROM
                          (SELECT tid AS orders_tid,
                                  orderdate AS o_orderdate
                           FROM orders_o_orderdate_run_1) AS X3
                        WHERE o_orderdate > '1995-03-15') AS U2 USING (orders_tid)) AS U2
                  WHERE U1.c_custkey=U2.o_custkey) U) AS U1
            JOIN
              (SELECT tid AS orders_tid,
                      orderkey AS o_orderkey
               FROM orders_o_orderkey_run_1) AS U2 USING (orders_tid)) AS U2
         WHERE U1.l_orderkey=U2.o_orderkey) U) AS U1
   JOIN
     (SELECT tid AS orders_tid,
             shippriority AS o_shippriority
      FROM orders_o_shippriority_run_1) AS U2 USING (orders_tid))X4;