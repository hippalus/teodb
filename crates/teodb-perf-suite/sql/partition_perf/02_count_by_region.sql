SELECT region, COUNT(*) AS cnt FROM perf.events GROUP BY region ORDER BY cnt DESC
