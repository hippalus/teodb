SELECT "order.payment_method", COUNT(*) AS cnt, AVG("order.total_amount") AS avg_amount, SUM("order.total_amount") AS total
FROM perf.events
WHERE event_type = 'order_placed'
GROUP BY "order.payment_method"
ORDER BY total DESC
