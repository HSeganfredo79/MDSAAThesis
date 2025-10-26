//April 2025 Whales by Unique Counterparties
WITH
  datetime({year: 2025, month: 4, day: 1, timezone: '-03:00'}) AS startDate,
  datetime({year: 2025, month: 5, day: 1, timezone: '-03:00'}) AS endDate

MATCH (w:Wallet)-[:SENT]->(tx:Transaction)-[:RECEIVED_BY]->(counter:Wallet)
WHERE tx.timestamp IS NOT NULL
  AND datetime({epochSeconds: tx.timestamp, timezone: '-03:00'}) >= startDate
  AND datetime({epochSeconds: tx.timestamp, timezone: '-03:00'}) < endDate

WITH w, count(DISTINCT counter) AS unique_peers
ORDER BY unique_peers DESC
LIMIT 10
RETURN w.address AS whale_address, unique_peers;
