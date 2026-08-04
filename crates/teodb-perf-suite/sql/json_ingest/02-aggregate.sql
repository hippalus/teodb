SELECT sensor_id, AVG(temperature) AS avg_temp
FROM synthetic_metrics
GROUP BY sensor_id
ORDER BY sensor_id
LIMIT 5
