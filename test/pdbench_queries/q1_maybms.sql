CREATE TABLE res_q1_maybms AS
SELECT l_orderkey,
       o_orderdate,
       o_shippriority
FROM
  (SELECT U1.C1 AS C1,
          U1.W1 AS W1,
          U1.C2 AS C2,
          U1.W2 AS W2,
          U1.C3 AS C3,
          U1.W3 AS W3,
          U1.C4 AS C4,
          U1.W4 AS W4,
          U1.C5 AS C5,
          U1.W5 AS W5,
          U1.C6 AS C6,
          U1.W6 AS W6,
          U1.C7 AS C7,
          U1.W7 AS W7,
          U2.C1 AS C8,
          U2.W1 AS W8,
          l_orderkey,
          o_orderdate,
          o_shippriority,
          lineitem_tid,
          customer_tid,
          orders_tid
   FROM
     (SELECT U.C1 AS C1,
             U.W1 AS W1,
             U.C2 AS C2,
             U.W2 AS W2,
             U.C3 AS C3,
             U.W3 AS W3,
             U.C4 AS C4,
             U.W4 AS W4,
             U.C5 AS C5,
             U.W5 AS W5,
             U.C6 AS C6,
             U.W6 AS W6,
             U.C7 AS C7,
             U.W7 AS W7,
             lineitem_tid,
             customer_tid,
             orders_tid,
             l_orderkey,
             o_orderdate
      FROM
        (SELECT U1.C1 AS C1,
                U1.W1 AS W1,
                U1.C2 AS C2,
                U1.W2 AS W2,
                U2.C1 AS C3,
                U2.W1 AS W3,
                U2.C2 AS C4,
                U2.W2 AS W4,
                U2.C3 AS C5,
                U2.W3 AS W5,
                U2.C4 AS C6,
                U2.W4 AS W6,
                U2.C5 AS C7,
                U2.W5 AS W7,
                l_orderkey,
                o_orderdate,
                o_orderkey,
                lineitem_tid,
                customer_tid,
                orders_tid
         FROM
           (SELECT U1.C1 AS C1,
                   U1.W1 AS W1,
                   U2.C1 AS C2,
                   U2.W1 AS W2,
                   l_orderkey,
                   lineitem_tid
            FROM
              (SELECT var_id AS C1,
                      world_id AS W1,
                      tid AS lineitem_tid,
                      orderkey AS l_orderkey
               FROM lineitem_l_orderkey) AS U1
            JOIN
              (SELECT U.C1 AS C1,
                      U.W1 AS W1,
                      lineitem_tid
               FROM
                 (SELECT *
                  FROM
                    (SELECT var_id AS C1,
                            world_id AS W1,
                            tid AS lineitem_tid,
                            shipdate AS l_shipdate
                     FROM lineitem_l_shipdate) AS X1
                  WHERE l_shipdate < '1995-03-17') U) AS U2 USING (lineitem_tid)
            WHERE (U1.C1 <> U2.C1
                   OR U1.W1 = U2.W1)) AS U1,

           (SELECT U1.C1 AS C1,
                   U1.W1 AS W1,
                   U1.C2 AS C2,
                   U1.W2 AS W2,
                   U1.C3 AS C3,
                   U1.W3 AS W3,
                   U1.C4 AS C4,
                   U1.W4 AS W4,
                   U2.C1 AS C5,
                   U2.W1 AS W5,
                   o_orderdate,
                   o_orderkey,
                   customer_tid,
                   orders_tid
            FROM
              (SELECT U.C1 AS C1,
                      U.W1 AS W1,
                      U.C2 AS C2,
                      U.W2 AS W2,
                      U.C3 AS C3,
                      U.W3 AS W3,
                      U.C4 AS C4,
                      U.W4 AS W4,
                      customer_tid,
                      orders_tid,
                      o_orderdate
               FROM
                 (SELECT U1.C1 AS C1,
                         U1.W1 AS W1,
                         U1.C2 AS C2,
                         U1.W2 AS W2,
                         U2.C1 AS C3,
                         U2.W1 AS W3,
                         U2.C2 AS C4,
                         U2.W2 AS W4,
                         c_custkey,
                         o_custkey,
                         o_orderdate,
                         customer_tid,
                         orders_tid
                  FROM
                    (SELECT U1.C1 AS C1,
                            U1.W1 AS W1,
                            U2.C1 AS C2,
                            U2.W1 AS W2,
                            c_custkey,
                            customer_tid
                     FROM
                       (SELECT var_id AS C1,
                               world_id AS W1,
                               tid AS customer_tid,
                               custkey AS c_custkey
                        FROM cust_c_custkey) AS U1
                     JOIN
                       (SELECT U.C1 AS C1,
                               U.W1 AS W1,
                               customer_tid
                        FROM
                          (SELECT *
                           FROM
                             (SELECT var_id AS C1,
                                     world_id AS W1,
                                     tid AS customer_tid,
                                     mktsegment AS c_mktsegment
                              FROM cust_c_mktsegment) AS X2
                           WHERE c_mktsegment = 'BUILDING') U) AS U2 USING (customer_tid)
                     WHERE (U1.C1 <> U2.C1
                            OR U1.W1 = U2.W1)) AS U1,

                    (SELECT U1.C1 AS C1,
                            U1.W1 AS W1,
                            U2.C1 AS C2,
                            U2.W1 AS W2,
                            o_custkey,
                            o_orderdate,
                            orders_tid
                     FROM
                       (SELECT var_id AS C1,
                               world_id AS W1,
                               tid AS orders_tid,
                               custkey AS o_custkey
                        FROM orders_o_custkey) AS U1
                     JOIN
                       (SELECT *
                        FROM
                          (SELECT var_id AS C1,
                                  world_id AS W1,
                                  tid AS orders_tid,
                                  orderdate AS o_orderdate
                           FROM orders_o_orderdate) AS X3
                        WHERE o_orderdate > '1995-03-15') AS U2 USING (orders_tid)
                     WHERE (U1.C1 <> U2.C1
                            OR U1.W1 = U2.W1)) AS U2
                  WHERE U1.c_custkey=U2.o_custkey
                    AND (U1.C2 <> U2.C2
                         OR U1.W2 = U2.W2)
                    AND (U1.C2 <> U2.C1
                         OR U1.W2 = U2.W1)
                    AND (U1.C1 <> U2.C2
                         OR U1.W1 = U2.W2)
                    AND (U1.C1 <> U2.C1
                         OR U1.W1 = U2.W1)) U) AS U1
            JOIN
              (SELECT var_id AS C1,
                      world_id AS W1,
                      tid AS orders_tid,
                      orderkey AS o_orderkey
               FROM orders_o_orderkey) AS U2 USING (orders_tid)
            WHERE (U1.C4 <> U2.C1
                   OR U1.W4 = U2.W1)
              AND (U1.C3 <> U2.C1
                   OR U1.W3 = U2.W1)
              AND (U1.C2 <> U2.C1
                   OR U1.W2 = U2.W1)
              AND (U1.C1 <> U2.C1
                   OR U1.W1 = U2.W1)) AS U2
         WHERE U1.l_orderkey=U2.o_orderkey
           AND (U1.C2 <> U2.C5
                OR U1.W2 = U2.W5)
           AND (U1.C2 <> U2.C4
                OR U1.W2 = U2.W4)
           AND (U1.C2 <> U2.C3
                OR U1.W2 = U2.W3)
           AND (U1.C2 <> U2.C2
                OR U1.W2 = U2.W2)
           AND (U1.C2 <> U2.C1
                OR U1.W2 = U2.W1)
           AND (U1.C1 <> U2.C5
                OR U1.W1 = U2.W5)
           AND (U1.C1 <> U2.C4
                OR U1.W1 = U2.W4)
           AND (U1.C1 <> U2.C3
                OR U1.W1 = U2.W3)
           AND (U1.C1 <> U2.C2
                OR U1.W1 = U2.W2)
           AND (U1.C1 <> U2.C1
                OR U1.W1 = U2.W1)) U) AS U1
   JOIN
     (SELECT var_id AS C1,
             world_id AS W1,
             tid AS orders_tid,
             shippriority AS o_shippriority
      FROM orders_o_shippriority) AS U2 USING (orders_tid)
   WHERE (U1.C7 <> U2.C1
          OR U1.W7 = U2.W1)
     AND (U1.C6 <> U2.C1
          OR U1.W6 = U2.W1)
     AND (U1.C5 <> U2.C1
          OR U1.W5 = U2.W1)
     AND (U1.C4 <> U2.C1
          OR U1.W4 = U2.W1)
     AND (U1.C3 <> U2.C1
          OR U1.W3 = U2.W1)
     AND (U1.C2 <> U2.C1
          OR U1.W2 = U2.W1)
     AND (U1.C1 <> U2.C1
          OR U1.W1 = U2.W1))X4;