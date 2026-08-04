SELECT passenger_count, COUNT(*) AS trips
FROM default.nyc_taxi
GROUP BY passenger_count
ORDER BY passenger_count
LIMIT 20;
