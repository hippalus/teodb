SELECT region, COUNT(*) AS cnt, SUM("order.total_amount") AS total_revenue
FROM perf.events
WHERE region = 'us-east'
  AND "timestamp" >= TIMESTAMP '2024-06-01 00:00:00'
  AND "timestamp" < TIMESTAMP '2024-07-01 00:00:00'
GROUP BY region
