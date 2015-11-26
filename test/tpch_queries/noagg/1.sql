SELECT returnflag, linestatus, 
  quantity AS sum_qty,
  extendedprice AS sum_base_price,
  extendedprice * (1-discount) AS sum_disc_price,
  extendedprice * (1-discount)*(1+tax) AS sum_charge,
  quantity AS avg_qty,
  extendedprice AS avg_price,
  discount AS avg_disc
FROM XLINEITEM
WHERE shipdate <= DATE('1997-09-01');
