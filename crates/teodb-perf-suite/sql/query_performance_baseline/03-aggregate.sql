SELECT l_returnflag, SUM(l_extendedprice) AS gross_revenue
FROM lineitem
WHERE l_shipdate >= DATE '1995-01-01'
  AND l_shipdate < DATE '1995-04-01'
GROUP BY l_returnflag
ORDER BY l_returnflag;
