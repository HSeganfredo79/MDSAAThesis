// April 2025 All-day breakdown WITH GAPS Consumed / Enriched / Scored
// TX-day window: adjust as needed
WITH datetime("2025-04-01T00:00:00Z") AS startDate,
     datetime("2025-05-01T00:00:00Z") AS endDate

MATCH (tx:Transaction)
WHERE tx.timestamp IS NOT NULL
  AND datetime({epochSeconds: tx.timestamp}) >= startDate
  AND datetime({epochSeconds: tx.timestamp}) < endDate
WITH
  date(datetime({epochSeconds: tx.timestamp})) AS tx_date,
  toFloat(tx.scoring) AS s,
  toInteger(tx.label)  AS lbl,
  (tx.consumed_at IS NOT NULL) AS consumed_f,
  (tx.enriched_at IS NOT NULL) AS enriched_f,
  (tx.scored_at   IS NOT NULL) AS scored_f
WITH
  tx_date,
  count(*) AS total,
  sum(CASE WHEN consumed_f THEN 1 ELSE 0 END) AS consumed,
  sum(CASE WHEN enriched_f THEN 1 ELSE 0 END) AS enriched,
  sum(CASE WHEN scored_f   THEN 1 ELSE 0 END) AS scored,
  min(CASE WHEN s IS NOT NULL AND lbl = 1 THEN s END) AS min_abnormal,
  max(CASE WHEN s IS NOT NULL AND lbl = 1 THEN s END) AS max_abnormal,
  min(CASE WHEN s IS NOT NULL AND lbl = 0 THEN s END) AS min_normal,
  max(CASE WHEN s IS NOT NULL AND lbl = 0 THEN s END) AS max_normal
// Shortest gap via adjacent pairs (no APOC)
CALL {
  WITH tx_date
  MATCH (t:Transaction)
  WHERE t.timestamp IS NOT NULL
    AND date(datetime({epochSeconds: t.timestamp})) = tx_date
    AND t.scoring IS NOT NULL
  WITH toFloat(t.scoring) AS s, toInteger(t.label) AS lbl
  ORDER BY s ASC
  WITH collect({s:s, lbl:lbl}) AS seq
  WITH seq, range(0, size(seq)-2) AS idx
  WITH [i IN idx WHERE seq[i].lbl <> seq[i+1].lbl | abs(seq[i+1].s - seq[i].s)] AS adj_diffs
  RETURN CASE WHEN size(adj_diffs) > 0
              THEN reduce(m=adj_diffs[0], x IN adj_diffs | CASE WHEN x < m THEN x ELSE m END)
              ELSE null END AS shortest_gap
}
WITH tx_date, total, consumed, enriched, scored,
     min_abnormal, max_abnormal, min_normal, max_normal, shortest_gap
RETURN
  tx_date,
  total,
  consumed,  CASE WHEN total>0 THEN round(toFloat(consumed)/total*100,2) ELSE 0 END AS pct_consumed,
  enriched,  CASE WHEN total>0 THEN round(toFloat(enriched)/total*100,2) ELSE 0 END AS pct_enriched,
  scored,    CASE WHEN total>0 THEN round(toFloat(scored)/total*100,2)   ELSE 0 END AS pct_scored,
  min_abnormal,
  max_abnormal,
  min_normal,
  max_normal,
  shortest_gap,
  CASE
    WHEN min_abnormal IS NOT NULL AND max_abnormal IS NOT NULL
     AND min_normal   IS NOT NULL AND max_normal   IS NOT NULL
    THEN
      CASE
        WHEN abs(max_abnormal - min_normal) >= abs(min_abnormal - max_normal)
        THEN abs(max_abnormal - min_normal)
        ELSE abs(min_abnormal - max_normal)
      END
    ELSE null
  END AS widest_gap
ORDER BY tx_date DESC;
