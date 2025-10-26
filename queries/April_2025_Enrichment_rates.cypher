// April 2025 Enrichment rates
WITH
  datetime('2025-04-01T00:00:00Z').epochSeconds AS aprStart,
  datetime('2025-05-01T00:00:00Z').epochSeconds AS aprEnd
MATCH (tx:Transaction)
WHERE tx.timestamp   IS NOT NULL
  AND tx.enriched_at IS NOT NULL
WITH
  // normalize original event time (timestamp) to epoch seconds
  CASE WHEN toFloat(tx.timestamp) > 2000000000
       THEN toInteger(round(toFloat(tx.timestamp)/1000.0))
       ELSE toInteger(toFloat(tx.timestamp)) END AS evt_sec,
  // normalize enriched_at to epoch seconds (accepts ms or s)
  CASE WHEN toFloat(tx.enriched_at) > 2000000000
       THEN toInteger(round(toFloat(tx.enriched_at)/1000.0))
       ELSE toInteger(toFloat(tx.enriched_at)) END AS enrich_sec,
  aprStart, aprEnd
WHERE evt_sec >= aprStart AND evt_sec < aprEnd
WITH count(*) AS n, min(enrich_sec) AS tmin, max(enrich_sec) AS tmax
WITH n, tmin, tmax,
     toFloat(CASE WHEN n = 0 OR tmin IS NULL OR tmax IS NULL
                  THEN 0 ELSE (tmax - tmin + 1) END) AS active_window_sec
RETURN
  n AS total_enriched_of_april_txs,
  CASE WHEN tmin IS NULL THEN NULL ELSE datetime({epochSeconds:tmin}) END AS first_enriched_at,
  CASE WHEN tmax IS NULL THEN NULL ELSE datetime({epochSeconds:tmax}) END AS last_enriched_at,
  active_window_sec AS active_window_seconds,
  CASE WHEN active_window_sec = 0 THEN 0 ELSE round(n / active_window_sec, 6) END AS enrich_rate_txs_per_sec,
  CASE WHEN n = 0 THEN NULL ELSE round(active_window_sec / n, 6) END AS avg_time_per_enrich_sec;
