CREATE TABLE res_q2_maybms AS
SELECT l_extendedprice
FROM
  (SELECT U1.C1 AS C1,
          U1.W1 AS W1,
          U1.C2 AS C2,
          U1.W2 AS W2,
          U2.C1 AS C3,
          U2.W1 AS W3,
          U2.C2 AS C4,
          U2.W2 AS W4,
          l_extendedprice,
          l_discount,
          lineitem_tid
   FROM
     (SELECT U1.C1 AS C1,
             U1.W1 AS W1,
             U2.C1 AS C2,
             U2.W1 AS W2,
             l_extendedprice,
             lineitem_tid
      FROM
        (SELECT U.C1 AS C1,
                U.W1 AS W1,
                lineitem_tid
         FROM
           (SELECT *
            FROM
              (SELECT var_id as C1,
                      world_id as W1,
                      tid AS lineitem_tid,
                      quantity as l_quantity
               FROM lineitem_l_quantity) AS X50
            WHERE l_quantity < '24') U) AS U1
      JOIN
        (SELECT var_id as C1,
                world_id as W1,
                tid AS lineitem_tid,
                extendedprice as l_extendedprice
         FROM lineitem_l_extendedprice) AS U2 USING (lineitem_tid)
      WHERE (U1.C1 <> U2.C1
             OR U1.W1 = U2.W1)) AS U1
   JOIN
     (SELECT U1.C1 AS C1,
             U1.W1 AS W1,
             U2.C1 AS C2,
             U2.W1 AS W2,
             l_discount,
             lineitem_tid
      FROM
        (SELECT U.C1 AS C1,
                U.W1 AS W1,
                lineitem_tid
         FROM
           (SELECT *
            FROM
              (SELECT var_id as C1,
                      world_id as W1,
                      tid AS lineitem_tid,
                      shipdate as l_shipdate
               FROM lineitem_l_shipdate) AS X51
            WHERE l_shipdate > '1994-01-01'
              AND l_shipdate < '1996-01-01') U) AS U1
      JOIN
        (SELECT *
         FROM
           (SELECT var_id as C1,
                   world_id as W1,
                   tid AS lineitem_tid,
                   discount as l_discount
            FROM lineitem_l_discount) AS X52
         WHERE l_discount > '0.05'
           AND l_discount < '0.08') AS U2 USING (lineitem_tid)
      WHERE (U1.C1 <> U2.C1
             OR U1.W1 = U2.W1)) AS U2 USING (lineitem_tid)
   WHERE (U1.C2 <> U2.C2
          OR U1.W2 = U2.W2)
     AND (U1.C2 <> U2.C1
          OR U1.W2 = U2.W1)
     AND (U1.C1 <> U2.C2
          OR U1.W1 = U2.W2)
     AND (U1.C1 <> U2.C1
          OR U1.W1 = U2.W1))X53;
