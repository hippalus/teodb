SELECT l_orderkey, l_partkey, l_shipdate
FROM lineitem
WHERE l_orderkey = 4
ORDER BY l_linenumber
LIMIT 5;
