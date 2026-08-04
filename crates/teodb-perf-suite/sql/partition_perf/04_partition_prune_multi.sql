SELECT region, event_type, COUNT(*) AS cnt
FROM perf.events
WHERE region IN ('us-east', 'eu-west')
GROUP BY region, event_type
ORDER BY cnt DESC
