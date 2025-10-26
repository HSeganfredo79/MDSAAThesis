// April 2025 USDC stats
WITH datetime("2025-04-01T00:00:00Z") AS startDate, datetime("2025-05-01T00:00:00Z") AS endDate
MATCH (tx:Transaction)
WHERE datetime({epochSeconds: tx.timestamp}) >= startDate AND datetime({epochSeconds: tx.timestamp}) < endDate
WITH tx.amount AS value
RETURN
  count(*) / 30.0 AS avg_tx_per_day,
  avg(value) AS avg_value,
  percentileCont(value, 0.5) AS median_value,
  max(value) AS max_value,
  min(value) AS min_value,
  stDev(value) AS stddev_value
