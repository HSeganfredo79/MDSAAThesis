//April Scoring Data Summaries 
WITH datetime({year:2025,month:4,day:1, hour:0,minute:0,second:0, timezone:'America/Sao_Paulo'}) AS startBR,
     datetime({year:2025,month:4,day:2, hour:0,minute:0,second:0, timezone:'America/Sao_Paulo'}) AS endBR
MATCH (tx:Transaction)
WHERE tx.timestamp IS NOT NULL
  AND datetime({epochSeconds: tx.timestamp, timezone:'America/Sao_Paulo'}) >= startBR
  AND datetime({epochSeconds: tx.timestamp, timezone:'America/Sao_Paulo'}) <  endBR
  AND tx.scoring IS NOT NULL
WITH "April 1st 2025" AS Period,
     count(tx) AS total,
     sum(CASE WHEN tx.scoring < 0 THEN 1 ELSE 0 END) AS anomalies,
     min(CASE WHEN tx.scoring < 0 THEN tx.scoring END) AS an_min,
     max(CASE WHEN tx.scoring < 0 THEN tx.scoring END) AS an_max,
     min(CASE WHEN tx.scoring > 0 THEN tx.scoring END) AS no_min,
     max(CASE WHEN tx.scoring > 0 THEN tx.scoring END) AS no_max
WITH Period, total, anomalies, an_min, an_max, no_min, no_max,
     CASE WHEN an_min IS NULL OR an_min = 0 THEN NULL ELSE floor(log10(abs(an_min))) END AS e_an_min,
     CASE WHEN an_max IS NULL OR an_max = 0 THEN NULL ELSE floor(log10(abs(an_max))) END AS e_an_max,
     CASE WHEN no_min IS NULL OR no_min = 0 THEN NULL ELSE floor(log10(abs(no_min))) END AS e_no_min,
     CASE WHEN no_max IS NULL OR no_max = 0 THEN NULL ELSE floor(log10(abs(no_max))) END AS e_no_max
RETURN
  Period,
  replace(toString(round(100.0 * anomalies / toFloat(total), 2)), ".", ",") + "%" AS `Anomaly Rate`,
  CASE
    WHEN an_min IS NULL THEN NULL
    WHEN an_min = 0 THEN "0"
    ELSE (CASE WHEN an_min < 0 THEN "-" ELSE "" END)
         + toString(round(abs(an_min) / exp(toFloat(e_an_min) * log(10.0)), 6)) + " × 10^" + toString(e_an_min)
  END AS `Anomaly Min Score`,
  CASE
    WHEN an_max IS NULL THEN NULL
    WHEN an_max = 0 THEN "0"
    ELSE (CASE WHEN an_max < 0 THEN "-" ELSE "" END)
         + toString(round(abs(an_max) / exp(toFloat(e_an_max) * log(10.0)), 6)) + " × 10^" + toString(e_an_max)
  END AS `Anomaly Max Score`,
  CASE
    WHEN no_min IS NULL THEN NULL
    WHEN no_min = 0 THEN "0"
    ELSE (CASE WHEN no_min < 0 THEN "-" ELSE "" END)
         + toString(round(abs(no_min) / exp(toFloat(e_no_min) * log(10.0)), 6)) + " × 10^" + toString(e_no_min)
  END AS `Normal Min Score`,
  CASE
    WHEN no_max IS NULL THEN NULL
    WHEN no_max = 0 THEN "0"
    ELSE (CASE WHEN no_max < 0 THEN "-" ELSE "" END)
         + toString(round(abs(no_max) / exp(toFloat(e_no_max) * log(10.0)), 6)) + " × 10^" + toString(e_no_max)
  END AS `Normal Max Score`

UNION ALL

// -------- Row 2: April 2025 (full month) in America/Sao_Paulo --------
WITH datetime({year:2025,month:4,day:1, hour:0,minute:0,second:0, timezone:'America/Sao_Paulo'}) AS startBR,
     datetime({year:2025,month:5,day:1, hour:0,minute:0,second:0, timezone:'America/Sao_Paulo'}) AS endBR
MATCH (tx:Transaction)
WHERE tx.timestamp IS NOT NULL
  AND datetime({epochSeconds: tx.timestamp, timezone:'America/Sao_Paulo'}) >= startBR
  AND datetime({epochSeconds: tx.timestamp, timezone:'America/Sao_Paulo'}) <  endBR
  AND tx.scoring IS NOT NULL
WITH "April 2025" AS Period,
     count(tx) AS total,
     sum(CASE WHEN tx.scoring < 0 THEN 1 ELSE 0 END) AS anomalies,
     min(CASE WHEN tx.scoring < 0 THEN tx.scoring END) AS an_min,
     max(CASE WHEN tx.scoring < 0 THEN tx.scoring END) AS an_max,
     min(CASE WHEN tx.scoring > 0 THEN tx.scoring END) AS no_min,
     max(CASE WHEN tx.scoring > 0 THEN tx.scoring END) AS no_max
WITH Period, total, anomalies, an_min, an_max, no_min, no_max,
     CASE WHEN an_min IS NULL OR an_min = 0 THEN NULL ELSE floor(log10(abs(an_min))) END AS e_an_min,
     CASE WHEN an_max IS NULL OR an_max = 0 THEN NULL ELSE floor(log10(abs(an_max))) END AS e_an_max,
     CASE WHEN no_min IS NULL OR no_min = 0 THEN NULL ELSE floor(log10(abs(no_min))) END AS e_no_min,
     CASE WHEN no_max IS NULL OR no_max = 0 THEN NULL ELSE floor(log10(abs(no_max))) END AS e_no_max
RETURN
  Period,
  replace(toString(round(100.0 * anomalies / toFloat(total), 2)), ".", ",") + "%" AS `Anomaly Rate`,
  CASE
    WHEN an_min IS NULL THEN NULL
    WHEN an_min = 0 THEN "0"
    ELSE (CASE WHEN an_min < 0 THEN "-" ELSE "" END)
         + toString(round(abs(an_min) / exp(toFloat(e_an_min) * log(10.0)), 6)) + " × 10^" + toString(e_an_min)
  END AS `Anomaly Min Score`,
  CASE
    WHEN an_max IS NULL THEN NULL
    WHEN an_max = 0 THEN "0"
    ELSE (CASE WHEN an_max < 0 THEN "-" ELSE "" END)
         + toString(round(abs(an_max) / exp(toFloat(e_an_max) * log(10.0)), 6)) + " × 10^" + toString(e_an_max)
  END AS `Anomaly Max Score`,
  CASE
    WHEN no_min IS NULL THEN NULL
    WHEN no_min = 0 THEN "0"
    ELSE (CASE WHEN no_min < 0 THEN "-" ELSE "" END)
         + toString(round(abs(no_min) / exp(toFloat(e_no_min) * log(10.0)), 6)) + " × 10^" + toString(e_no_min)
  END AS `Normal Min Score`,
  CASE
    WHEN no_max IS NULL THEN NULL
    WHEN no_max = 0 THEN "0"
    ELSE (CASE WHEN no_max < 0 THEN "-" ELSE "" END)
         + toString(round(abs(no_max) / exp(toFloat(e_no_max) * log(10.0)), 6)) + " × 10^" + toString(e_no_max)
  END AS `Normal Max Score`;
