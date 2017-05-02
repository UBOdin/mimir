CREATE TABLE res_q3_mimir AS
SELECT n_name,
       n_name2
FROM
  (SELECT U1.n_name,
          U2.n_name AS n_name2,
          lineitem_tid,
          supplier_tid,
          U1.nation_tid,
          customer_tid,
          orders_tid,
          U2.nation_tid AS nation_tid2
   FROM
     (SELECT n_name,
             orders_tid,
             lineitem_tid,
             supplier_tid,
             nation_tid
      FROM
        (SELECT orders_tid,
                lineitem_tid,
                supplier_tid,
                nation_tid
         FROM
           (SELECT supplier_tid,
                   orders_tid,
                   lineitem_tid
            FROM
              (SELECT supplier_tid,
                      lineitem_tid
               FROM
                 (SELECT s_suppkey,
                         l_suppkey,
                         supplier_tid,
                         lineitem_tid
                  FROM
                    (SELECT tid AS supplier_tid,
                            suppkey as s_suppkey
                     FROM supp_s_suppkey_run_1) AS U1,

                    (SELECT tid AS lineitem_tid,
                            suppkey as l_suppkey
                     FROM lineitem_l_suppkey_run_1) AS U2
                  WHERE U1.s_suppkey=U2.l_suppkey) U) AS U1
            JOIN
              (SELECT orders_tid,
                      lineitem_tid
               FROM
                 (SELECT o_orderkey,
                         l_orderkey,
                         orders_tid,
                         lineitem_tid
                  FROM
                    (SELECT tid AS orders_tid,
                            orderkey as o_orderkey
                     FROM orders_o_orderkey_run_1) AS U1,

                    (SELECT tid AS lineitem_tid,
                            orderkey as l_orderkey
                     FROM lineitem_l_orderkey_run_1) AS U2
                  WHERE U1.o_orderkey=U2.l_orderkey) U) AS U2 USING (lineitem_tid)
           ) AS U1
         JOIN
           (SELECT supplier_tid,
                   nation_tid
            FROM
              (SELECT s_nationkey,
                      n_nationkey,
                      supplier_tid,
                      nation_tid
               FROM
                 (SELECT tid AS supplier_tid,
                         nationkey as s_nationkey
                  FROM supp_s_nationkey_run_1) AS U1,

                 (SELECT tid AS nation_tid,
                         nationkey as n_nationkey
                  FROM nation_n_nationkey_run_1) AS U2
               WHERE U1.s_nationkey=U2.n_nationkey) U) AS U2 USING (supplier_tid)
        ) AS U1
      JOIN
        (SELECT *
         FROM
           (SELECT tid AS nation_tid,
                   name as n_name
            FROM nation_n_name_run_1) AS X4
         WHERE n_name = 'GERMANY') AS U2 USING (nation_tid)
      ) AS U1
   JOIN
     (SELECT n_name,
             nation_tid,
             customer_tid,
             orders_tid
      FROM
        (SELECT nation_tid,
                customer_tid,
                orders_tid
         FROM
           (SELECT customer_tid,
                   nation_tid
            FROM
              (SELECT c_nationkey,
                      n_nationkey,
                      customer_tid,
                      nation_tid
               FROM
                 (SELECT tid AS customer_tid,
                         nationkey as c_nationkey
                  FROM cust_c_nationkey_run_1) AS U1,

                 (SELECT tid AS nation_tid,
                         nationkey as n_nationkey
                  FROM nation_n_nationkey_run_1) AS U2
               WHERE U1.c_nationkey=U2.n_nationkey) U) AS U1
         JOIN
           (SELECT customer_tid,
                   orders_tid
            FROM
              (SELECT c_custkey,
                      o_custkey,
                      customer_tid,
                      orders_tid
               FROM
                 (SELECT tid AS customer_tid,
                         custkey as c_custkey
                  FROM cust_c_custkey_run_1) AS U1,

                 (SELECT tid AS orders_tid,
                         custkey as o_custkey
                  FROM orders_o_custkey_run_1) AS U2
               WHERE U1.c_custkey=U2.o_custkey) U) AS U2 USING (customer_tid)
        ) AS U1
      JOIN
        (SELECT *
         FROM
           (SELECT tid AS nation_tid,
                   name as n_name
            FROM nation_n_name_run_1) AS X5
         WHERE n_name = 'IRAQ') AS U2 USING (nation_tid)
      ) AS U2 USING (orders_tid)
  )X6;