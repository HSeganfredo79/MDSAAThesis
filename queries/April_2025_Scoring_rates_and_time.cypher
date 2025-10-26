//April 2025 Scoring rates and time
WITH
  datetime('2025-04-01T00:00:00Z').epochSeconds AS aprStart,
  datetime('2025-05-01T00:00:00Z').epochSeconds AS aprEnd
MATCH (tx:Transaction)
WHERE tx.timestamp IS NOT NULL
  AND tx.scored_at IS NOT NULL
WITH tx, aprStart, aprEnd,
     // normalize event time (timestamp) to epoch seconds
     (CASE WHEN toFloat(tx.timestamp) > 2000000000
           THEN toInteger(round(toFloat(tx.timestamp)/1000.0))
           ELSE toInteger(toFloat(tx.timestamp))
      END) AS evt_sec,
     // normalize scored_at to epoch seconds
     (CASE WHEN toFloat(tx.scored_at) > 2000000000
           THEN toInteger(round(toFloat(tx.scored_at)/1000.0))
           ELSE toInteger(toFloat(tx.scored_at))
      END) AS score_sec
WHERE evt_sec >= aprStart AND evt_sec < aprEnd     // <-- ONLY April filter
WITH collect(score_sec) AS scores, aprStart, aprEnd
WITH size(scores) AS n,
     (CASE WHEN size(scores)=0 THEN NULL ELSE apoc.coll.min(scores) END) AS tmin,
     (CASE WHEN size(scores)=0 THEN NULL ELSE apoc.coll.max(scores) END) AS tmax,
     aprStart, aprEnd
WITH n, tmin, tmax,
     // active window over when scoring actually happened for those April txs
     toFloat(CASE WHEN n=0 OR tmin IS NULL OR tmax IS NULL THEN 0 ELSE (tmax - tmin + 1) END) AS active_window_sec
RETURN
  n AS total_scored_of_april_txs,
  CASE WHEN tmin IS NULL THEN NULL ELSE datetime({epochSeconds:tmin}) END AS first_scored_at,
  CASE WHEN tmax IS NULL THEN NULL ELSE datetime({epochSeconds:tmax}) END AS last_scored_at,
  active_window_sec AS active_window_seconds,
  CASE WHEN active_window_sec = 0 THEN 0 ELSE round(n / active_window_sec, 6) END AS scoring_rate_txs_per_sec,
  CASE WHEN n = 0 THEN NULL ELSE round(active_window_sec / n, 6) END AS avg_time_per_scoring_sec;
