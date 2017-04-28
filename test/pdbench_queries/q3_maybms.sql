CREATE TABLE res_q3_maybms AS
SELECT n_name,
       n_name2
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
          U2.C2 AS C9,
          U2.W2 AS W9,
          U2.C3 AS C10,
          U2.W3 AS W10,
          U2.C4 AS C11,
          U2.W4 AS W11,
          U2.C5 AS C12,
          U2.W5 AS W12,
          U1.n_name,
          U2.n_name AS n_name2,
          lineitem_tid,
          supplier_tid,
          U1.nation_tid,
          customer_tid,
          orders_tid,
          U2.nation_tid AS nation_tid2
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
             U2.C1 AS C7,
             U2.W1 AS W7,
             n_name,
             orders_tid,
             lineitem_tid,
             supplier_tid,
             nation_tid
      FROM
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
                U2.C2 AS C6,
                U2.W2 AS W6,
                orders_tid,
                lineitem_tid,
                supplier_tid,
                nation_tid
         FROM
           (SELECT U1.C1 AS C1,
                   U1.W1 AS W1,
                   U1.C2 AS C2,
                   U1.W2 AS W2,
                   U2.C1 AS C3,
                   U2.W1 AS W3,
                   U2.C2 AS C4,
                   U2.W2 AS W4,
                   supplier_tid,
                   orders_tid,
                   lineitem_tid
            FROM
              (SELECT U.C1 AS C1,
                      U.W1 AS W1,
                      U.C2 AS C2,
                      U.W2 AS W2,
                      supplier_tid,
                      lineitem_tid
               FROM
                 (SELECT U1.C1 AS C1,
                         U1.W1 AS W1,
                         U2.C1 AS C2,
                         U2.W1 AS W2,
                         s_suppkey,
                         l_suppkey,
                         supplier_tid,
                         lineitem_tid
                  FROM
                    (SELECT var_id as C1,
                            world_id as W1,
                            tid AS supplier_tid,
                            suppkey as s_suppkey
                     FROM supp_s_suppkey) AS U1,

                    (SELECT var_id as C1,
                            world_id as W1,
                            tid AS lineitem_tid,
                            suppkey as l_suppkey
                     FROM lineitem_l_suppkey) AS U2
                  WHERE U1.s_suppkey=U2.l_suppkey
                    AND (U1.C1 <> U2.C1
                         OR U1.W1 = U2.W1)) U) AS U1
            JOIN
              (SELECT U.C1 AS C1,
                      U.W1 AS W1,
                      U.C2 AS C2,
                      U.W2 AS W2,
                      orders_tid,
                      lineitem_tid
               FROM
                 (SELECT U1.C1 AS C1,
                         U1.W1 AS W1,
                         U2.C1 AS C2,
                         U2.W1 AS W2,
                         o_orderkey,
                         l_orderkey,
                         orders_tid,
                         lineitem_tid
                  FROM
                    (SELECT var_id as C1,
                            world_id as W1,
                            tid AS orders_tid,
                            orderkey as o_orderkey
                     FROM orders_o_orderkey) AS U1,

                    (SELECT var_id as C1,
                            world_id as W1,
                            tid AS lineitem_tid,
                            orderkey as l_orderkey
                     FROM lineitem_l_orderkey) AS U2
                  WHERE U1.o_orderkey=U2.l_orderkey
                    AND (U1.C1 <> U2.C1
                         OR U1.W1 = U2.W1)) U) AS U2 USING (lineitem_tid)
            WHERE (U1.C2 <> U2.C2
                   OR U1.W2 = U2.W2)
              AND (U1.C2 <> U2.C1
                   OR U1.W2 = U2.W1)
              AND (U1.C1 <> U2.C2
                   OR U1.W1 = U2.W2)
              AND (U1.C1 <> U2.C1
                   OR U1.W1 = U2.W1)) AS U1
         JOIN
           (SELECT U.C1 AS C1,
                   U.W1 AS W1,
                   U.C2 AS C2,
                   U.W2 AS W2,
                   supplier_tid,
                   nation_tid
            FROM
              (SELECT U1.C1 AS C1,
                      U1.W1 AS W1,
                      U2.C1 AS C2,
                      U2.W1 AS W2,
                      s_nationkey,
                      n_nationkey,
                      supplier_tid,
                      nation_tid
               FROM
                 (SELECT var_id as C1,
                         world_id as W1,
                         tid AS supplier_tid,
                         nationkey as s_nationkey
                  FROM supp_s_nationkey) AS U1,

                 (SELECT var_id as C1,
                         world_id as W1,
                         tid AS nation_tid,
                         nationkey as n_nationkey
                  FROM nation_n_nationkey) AS U2
               WHERE U1.s_nationkey=U2.n_nationkey
                 AND (U1.C1 <> U2.C1
                      OR U1.W1 = U2.W1)) U) AS U2 USING (supplier_tid)
         WHERE (U1.C4 <> U2.C2
                OR U1.W4 = U2.W2)
           AND (U1.C4 <> U2.C1
                OR U1.W4 = U2.W1)
           AND (U1.C3 <> U2.C2
                OR U1.W3 = U2.W2)
           AND (U1.C3 <> U2.C1
                OR U1.W3 = U2.W1)
           AND (U1.C2 <> U2.C2
                OR U1.W2 = U2.W2)
           AND (U1.C2 <> U2.C1
                OR U1.W2 = U2.W1)
           AND (U1.C1 <> U2.C2
                OR U1.W1 = U2.W2)
           AND (U1.C1 <> U2.C1
                OR U1.W1 = U2.W1)) AS U1
      JOIN
        (SELECT *
         FROM
           (SELECT var_id as C1,
                   world_id as W1,
                   tid AS nation_tid,
                   name as n_name
            FROM nation_n_name) AS X4
         WHERE n_name = 'GERMANY') AS U2 USING (nation_tid)
      WHERE (U1.C6 <> U2.C1
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
             OR U1.W1 = U2.W1)) AS U1
   JOIN
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
             n_name,
             nation_tid,
             customer_tid,
             orders_tid
      FROM
        (SELECT U1.C1 AS C1,
                U1.W1 AS W1,
                U1.C2 AS C2,
                U1.W2 AS W2,
                U2.C1 AS C3,
                U2.W1 AS W3,
                U2.C2 AS C4,
                U2.W2 AS W4,
                nation_tid,
                customer_tid,
                orders_tid
         FROM
           (SELECT U.C1 AS C1,
                   U.W1 AS W1,
                   U.C2 AS C2,
                   U.W2 AS W2,
                   customer_tid,
                   nation_tid
            FROM
              (SELECT U1.C1 AS C1,
                      U1.W1 AS W1,
                      U2.C1 AS C2,
                      U2.W1 AS W2,
                      c_nationkey,
                      n_nationkey,
                      customer_tid,
                      nation_tid
               FROM
                 (SELECT var_id as C1,
                         world_id as W1,
                         tid AS customer_tid,
                         nationkey as c_nationkey
                  FROM cust_c_nationkey) AS U1,

                 (SELECT var_id as C1,
                         world_id as W1,
                         tid AS nation_tid,
                         nationkey as n_nationkey
                  FROM nation_n_nationkey) AS U2
               WHERE U1.c_nationkey=U2.n_nationkey
                 AND (U1.C1 <> U2.C1
                      OR U1.W1 = U2.W1)) U) AS U1
         JOIN
           (SELECT U.C1 AS C1,
                   U.W1 AS W1,
                   U.C2 AS C2,
                   U.W2 AS W2,
                   customer_tid,
                   orders_tid
            FROM
              (SELECT U1.C1 AS C1,
                      U1.W1 AS W1,
                      U2.C1 AS C2,
                      U2.W1 AS W2,
                      c_custkey,
                      o_custkey,
                      customer_tid,
                      orders_tid
               FROM
                 (SELECT var_id as C1,
                         world_id as W1,
                         tid AS customer_tid,
                         custkey as c_custkey
                  FROM cust_c_custkey) AS U1,

                 (SELECT var_id as C1,
                         world_id as W1,
                         tid AS orders_tid,
                         custkey as o_custkey
                  FROM orders_o_custkey) AS U2
               WHERE U1.c_custkey=U2.o_custkey
                 AND (U1.C1 <> U2.C1
                      OR U1.W1 = U2.W1)) U) AS U2 USING (customer_tid)
         WHERE (U1.C2 <> U2.C2
                OR U1.W2 = U2.W2)
           AND (U1.C2 <> U2.C1
                OR U1.W2 = U2.W1)
           AND (U1.C1 <> U2.C2
                OR U1.W1 = U2.W2)
           AND (U1.C1 <> U2.C1
                OR U1.W1 = U2.W1)) AS U1
      JOIN
        (SELECT *
         FROM
           (SELECT var_id as C1,
                   world_id as W1,
                   tid AS nation_tid,
                   name as n_name
            FROM nation_n_name) AS X5
         WHERE n_name = 'IRAQ') AS U2 USING (nation_tid)
      WHERE (U1.C4 <> U2.C1
             OR U1.W4 = U2.W1)
        AND (U1.C3 <> U2.C1
             OR U1.W3 = U2.W1)
        AND (U1.C2 <> U2.C1
             OR U1.W2 = U2.W1)
        AND (U1.C1 <> U2.C1
             OR U1.W1 = U2.W1)) AS U2 USING (orders_tid)
   WHERE (U1.C7 <> U2.C5
          OR U1.W7 = U2.W5)
     AND (U1.C7 <> U2.C4
          OR U1.W7 = U2.W4)
     AND (U1.C7 <> U2.C3
          OR U1.W7 = U2.W3)
     AND (U1.C7 <> U2.C2
          OR U1.W7 = U2.W2)
     AND (U1.C7 <> U2.C1
          OR U1.W7 = U2.W1)
     AND (U1.C6 <> U2.C5
          OR U1.W6 = U2.W5)
     AND (U1.C6 <> U2.C4
          OR U1.W6 = U2.W4)
     AND (U1.C6 <> U2.C3
          OR U1.W6 = U2.W3)
     AND (U1.C6 <> U2.C2
          OR U1.W6 = U2.W2)
     AND (U1.C6 <> U2.C1
          OR U1.W6 = U2.W1)
     AND (U1.C5 <> U2.C5
          OR U1.W5 = U2.W5)
     AND (U1.C5 <> U2.C4
          OR U1.W5 = U2.W4)
     AND (U1.C5 <> U2.C3
          OR U1.W5 = U2.W3)
     AND (U1.C5 <> U2.C2
          OR U1.W5 = U2.W2)
     AND (U1.C5 <> U2.C1
          OR U1.W5 = U2.W1)
     AND (U1.C4 <> U2.C5
          OR U1.W4 = U2.W5)
     AND (U1.C4 <> U2.C4
          OR U1.W4 = U2.W4)
     AND (U1.C4 <> U2.C3
          OR U1.W4 = U2.W3)
     AND (U1.C4 <> U2.C2
          OR U1.W4 = U2.W2)
     AND (U1.C4 <> U2.C1
          OR U1.W4 = U2.W1)
     AND (U1.C3 <> U2.C5
          OR U1.W3 = U2.W5)
     AND (U1.C3 <> U2.C4
          OR U1.W3 = U2.W4)
     AND (U1.C3 <> U2.C3
          OR U1.W3 = U2.W3)
     AND (U1.C3 <> U2.C2
          OR U1.W3 = U2.W2)
     AND (U1.C3 <> U2.C1
          OR U1.W3 = U2.W1)
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
          OR U1.W1 = U2.W1))X6;