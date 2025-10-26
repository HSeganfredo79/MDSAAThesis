// April 2025 Whale Market Volume
// 2025-04-01T00:00:00Z UTC-3 => 1743476400
// 2025-04-02T00:00:00Z UTC-3 => 1746068400
WITH 1743476400 AS startEpoch, 1746068400 AS endEpoch
MATCH (tx:Transaction)
WHERE tx.timestamp >= startEpoch AND tx.timestamp < endEpoch
OPTIONAL MATCH (src:Wallet)-[:SENT]->(tx)
OPTIONAL MATCH (tx)-[:RECEIVED_BY]->(dst:Wallet)
WITH
  (CASE WHEN src IS NULL THEN [] ELSE [{address: src.address, vol: toFloat(tx.amount)}] END) +
  (CASE WHEN dst IS NULL THEN [] ELSE [{address: dst.address, vol: toFloat(tx.amount)}] END) AS rows
UNWIND rows AS r
WITH r.address AS address, sum(r.vol) AS total_volume
ORDER BY total_volume DESC
LIMIT 10
RETURN address AS whale_address, total_volume;