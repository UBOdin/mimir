CREATE TABLE res_q2_maybms AS
SELECT l_extendedprice
FROM
  (SELECT l_extendedprice,
          l_discount,
          lineitem_tid
   FROM
     (SELECT l_extendedprice,
             lineitem_tid
      FROM
        (SELECT lineitem_tid
         FROM
           (SELECT *
            FROM
              (SELECT tid AS lineitem_tid,
                      quantity as l_quantity
               FROM lineitem_l_quantity_run_1) AS X50
            WHERE l_quantity < '24') U) AS U1
      JOIN
        (SELECT tid AS lineitem_tid,
                extendedprice as l_extendedprice
         FROM lineitem_l_extendedprice_run_1) AS U2 USING (lineitem_tid)
    ) AS U1
   JOIN
     (SELECT l_discount,
             lineitem_tid
      FROM
        (SELECT lineitem_tid
         FROM
           (SELECT *
            FROM
              (SELECT tid AS lineitem_tid,
                      shipdate as l_shipdate
               FROM lineitem_l_shipdate_run_1) AS X51
            WHERE l_shipdate > '1994-01-01'
              AND l_shipdate < '1996-01-01') U) AS U1
      JOIN
        (SELECT *
         FROM
           (SELECT tid AS lineitem_tid,
                   discount as l_discount
            FROM lineitem_l_discount_run_1) AS X52
         WHERE l_discount > '0.05'
           AND l_discount < '0.08') AS U2 USING (lineitem_tid)
    ) AS U2 USING (lineitem_tid)
   )X53;
