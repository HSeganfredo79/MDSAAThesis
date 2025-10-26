//April 2025 Phase1/Phase2 Diffs
WITH datetime("2025-04-01T00:00:00Z") AS startDate,
     datetime("2025-05-01T00:00:00Z") AS endDate
MATCH (tx:Transaction)
WHERE tx.timestamp >= startDate.epochSeconds
  AND tx.timestamp <  endDate.epochSeconds
  AND tx.consumed_at IS NOT NULL
WITH date(datetime({epochSeconds: tx.timestamp})) AS tx_day, tx
WITH tx_day,
     tx,
     (tx.scoring    IS NOT NULL) AS has_v1,
     (tx.scoring_v2 IS NOT NULL) AS has_v2
WITH tx_day,
     sum(CASE WHEN has_v1 THEN 1 ELSE 0 END) AS v1_total,
     sum(CASE WHEN has_v1 AND has_v2 THEN 1 ELSE 0 END) AS both_total,
     sum(CASE WHEN has_v1 AND NOT has_v2 THEN 1 ELSE 0 END) AS v1_without_v2,
     sum(CASE WHEN has_v2 AND NOT has_v1 THEN 1 ELSE 0 END) AS v2_without_v1,
     collect(CASE WHEN has_v1 AND NOT has_v2 THEN tx.transaction_id END)[..20] AS sample_v1_missing_v2
RETURN
  tx_day,
  v1_total,
  both_total,
  v1_without_v2,
  round(100.0 * both_total / CASE WHEN v1_total = 0 THEN 1 ELSE v1_total END, 2) AS pct_v1_covered_by_v2,
  v2_without_v1,
  sample_v1_missing_v2
ORDER BY tx_day ASC;
