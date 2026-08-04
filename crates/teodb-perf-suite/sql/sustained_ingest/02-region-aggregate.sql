SELECT region, sensor_id, AVG(value) AS avg_value, COUNT(*) AS n
FROM sustained_metrics
GROUP BY region, sensor_id
ORDER BY region, sensor_id
LIMIT 20
