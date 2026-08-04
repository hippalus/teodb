SELECT COUNT(*) AS january_orders
FROM orders
WHERE o_orderdate >= DATE '1994-01-01'
  AND o_orderdate < DATE '1994-02-01';
