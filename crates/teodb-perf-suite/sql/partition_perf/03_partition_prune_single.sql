SELECT COUNT(*) AS cnt, AVG("order.total_amount") AS avg_amount
FROM perf.events
WHERE region = 'us-east'
