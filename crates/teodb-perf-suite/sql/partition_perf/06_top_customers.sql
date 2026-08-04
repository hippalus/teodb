SELECT "customer.id", "customer.tier", COUNT(*) AS order_count, SUM("order.total_amount") AS total_spent
FROM perf.events
WHERE region = 'eu-west'
  AND event_type = 'order_placed'
GROUP BY "customer.id", "customer.tier"
ORDER BY total_spent DESC
LIMIT 20
