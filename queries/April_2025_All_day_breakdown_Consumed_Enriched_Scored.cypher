// April 2025 All-day breakdown Consumed / Enriched / Scored
WITH datetime("2025-04-01T00:00:00Z") AS startDate, datetime("2025-05-01T00:00:00Z") AS endDate
MATCH (tx:Transaction)
WHERE datetime({epochSeconds: tx.timestamp}) >= startDate AND datetime({epochSeconds: tx.timestamp}) < endDate
WITH date(datetime({epochSeconds: tx.timestamp})) AS tx_date,
     count(tx) AS total,
     count(CASE WHEN tx.consumed_at IS NOT NULL THEN 1 END) AS consumed,
     count(CASE WHEN tx.enriched_at IS NOT NULL THEN 1 END) AS enriched,
     count(CASE WHEN tx.scored_at IS NOT NULL THEN 1 END) AS scored
RETURN
  tx_date,
  total,
  consumed,
  round(toFloat(consumed) / total * 100, 2) AS pct_consumed,
  enriched,
  round(toFloat(enriched) / total * 100, 2) AS pct_enriched,
  scored,
  round(toFloat(scored) / total * 100, 2) AS pct_scored
ORDER BY tx_date DESC
