WITH revenue0 AS (
    SELECT
        l_suppkey AS supplier_no,
        sum(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM tpch.lineitem
    WHERE l_shipdate >= date '1996-01-01'
      AND l_shipdate < date '1996-01-01' + interval '3 month'
    GROUP BY l_suppkey
)
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM tpch.supplier, revenue0
WHERE s_suppkey = supplier_no
  AND total_revenue = (SELECT max(total_revenue) FROM revenue0)
ORDER BY s_suppkey;
