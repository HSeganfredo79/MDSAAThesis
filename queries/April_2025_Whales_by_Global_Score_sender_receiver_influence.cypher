// April 2025 Whales by Global Score (sender + receiver influence)
WITH
  datetime({year: 2025, month: 4, day: 1, timezone: '-03:00'}) AS startDate,
  datetime({year: 2025, month: 5, day: 1, timezone: '-03:00'}) AS endDate
WITH startDate.epochSeconds AS startEpoch, endDate.epochSeconds AS endEpoch

// 1) Aggregate outgoing influence in the window
CALL {
  WITH startEpoch, endEpoch
  MATCH (w:Wallet)-[:SENT]->(tx:Transaction)
  WHERE tx.timestamp >= startEpoch AND tx.timestamp < endEpoch
  RETURN
    w.address AS address,
    sum(coalesce(tx.from_pagerank,   0)) AS out_pagerank,
    sum(coalesce(tx.from_centrality, 0)) AS out_centrality
}

// Collect to avoid cartesian products
WITH startEpoch, endEpoch,
     collect({address: address, out_pagerank: out_pagerank, out_centrality: out_centrality}) AS outs

// 2) Aggregate incoming influence in the window
CALL {
  WITH startEpoch, endEpoch
  MATCH (w:Wallet)<-[:RECEIVED_BY]-(tx:Transaction)
  WHERE tx.timestamp >= startEpoch AND tx.timestamp < endEpoch
  RETURN
    w.address AS address,
    sum(coalesce(tx.to_pagerank,   0)) AS in_pagerank,
    sum(coalesce(tx.to_centrality, 0)) AS in_centrality
}

WITH outs,
     collect({address: address, in_pagerank: in_pagerank, in_centrality: in_centrality}) AS ins

// 3) Merge the two result sets by address
WITH outs + ins AS rows
UNWIND rows AS r
WITH r.address AS address,
     coalesce(max(r.out_pagerank),   0) AS out_pagerank,
     coalesce(max(r.out_centrality), 0) AS out_centrality,
     coalesce(max(r.in_pagerank),    0) AS in_pagerank,
     coalesce(max(r.in_centrality),  0) AS in_centrality
WITH address, out_pagerank, out_centrality, in_pagerank, in_centrality,
     (out_pagerank + out_centrality + in_pagerank + in_centrality) AS total_influence
ORDER BY total_influence DESC
LIMIT 10
RETURN address AS whale_address,
       out_pagerank, out_centrality,
       in_pagerank, in_centrality,
       total_influence;
