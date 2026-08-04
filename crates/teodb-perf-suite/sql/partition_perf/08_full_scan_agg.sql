SELECT
    region,
    "customer.tier",
    COUNT(*) AS events,
    AVG("order.total_amount") AS avg_amount,
    MIN("order.total_amount") AS min_amount,
    MAX("order.total_amount") AS max_amount
FROM perf.events
GROUP BY region, "customer.tier"
ORDER BY region, events DESC
