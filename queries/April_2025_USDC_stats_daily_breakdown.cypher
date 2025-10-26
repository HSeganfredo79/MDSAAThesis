// April 2025 USDC stats (daily breakdown)
WITH datetime("2025-04-01T00:00:00Z") AS startDate, datetime("2025-05-01T00:00:00Z") AS endDate
MATCH (tx:Transaction)
WHERE datetime({epochSeconds: tx.timestamp}) >= startDate AND datetime({epochSeconds: tx.timestamp}) < endDate
WITH date(datetime({epochSeconds: tx.timestamp})) AS txDate, tx.amount AS value
RETURN 
  txDate,
  count(*) AS num_tx,
  sum(value) AS total_value,
  avg(value) AS avg_value,
  percentileCont(value, 0.5) AS median_value
ORDER BY txDate
