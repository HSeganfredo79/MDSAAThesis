// April 2025 Processing rates for April 2025 (hour/minute/second)
WITH
  datetime("2025-04-01T00:00:00Z") AS startDate,
  datetime("2025-05-01T00:00:00Z") AS endDate

// ---- Hourly stats
CALL {
  WITH startDate, endDate
  MATCH (tx:Transaction)
  WHERE datetime({epochSeconds: tx.timestamp}) >= startDate
    AND datetime({epochSeconds: tx.timestamp}) < endDate
  WITH datetime({epochSeconds: tx.timestamp}) AS dt
  WITH date(dt) AS d, dt.hour AS h, count(*) AS c
  RETURN
    min(c)  AS low_hour,
    avg(c)  AS avg_hour,
    max(c)  AS peak_hour,
    stDev(c) AS std_hour
}

// ---- Minute stats
CALL {
  WITH startDate, endDate
  MATCH (tx:Transaction)
  WHERE datetime({epochSeconds: tx.timestamp}) >= startDate
    AND datetime({epochSeconds: tx.timestamp}) < endDate
  WITH datetime({epochSeconds: tx.timestamp}) AS dt
  WITH date(dt) AS d, dt.hour AS h, dt.minute AS m, count(*) AS c
  RETURN
    min(c)  AS low_min,
    avg(c)  AS avg_min,
    max(c)  AS peak_min,
    stDev(c) AS std_min
}

// ---- Second stats
CALL {
  WITH startDate, endDate
  MATCH (tx:Transaction)
  WHERE datetime({epochSeconds: tx.timestamp}) >= startDate
    AND datetime({epochSeconds: tx.timestamp}) < endDate
  WITH datetime({epochSeconds: tx.timestamp}) AS dt
  WITH date(dt) AS d, dt.hour AS h, dt.minute AS m, dt.second AS s, count(*) AS c
  RETURN
    min(c)  AS low_sec,
    avg(c)  AS avg_sec,
    max(c)  AS peak_sec,
    stDev(c) AS std_sec
}

RETURN
  low_hour,  avg_hour,  peak_hour,  std_hour,
  low_min,   avg_min,   peak_min,   std_min,
  low_sec,   avg_sec,   peak_sec,   std_sec;

