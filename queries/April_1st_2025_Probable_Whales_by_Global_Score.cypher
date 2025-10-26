// April 1st 2025 Probable Whales by Global Score
WITH datetime("2025-04-01T00:00:00Z") AS startDate, datetime("2025-04-02T00:00:00Z") AS endDate
MATCH (w:Wallet)
OPTIONAL MATCH (w)-[:SENT]->(tx_out:Transaction)
  WHERE datetime({epochSeconds: tx_out.timestamp}) >= startDate 
    AND datetime({epochSeconds: tx_out.timestamp}) < endDate
OPTIONAL MATCH (tx_in:Transaction)-[:RECEIVED_BY]->(w)
  WHERE datetime({epochSeconds: tx_in.timestamp}) >= startDate 
    AND datetime({epochSeconds: tx_in.timestamp}) < endDate
WITH w,
  sum(coalesce(tx_out.from_pagerank, 0)) AS out_pagerank,
  sum(coalesce(tx_out.from_centrality, 0)) AS out_centrality,
  sum(coalesce(tx_in.to_pagerank, 0)) AS in_pagerank,
  sum(coalesce(tx_in.to_centrality, 0)) AS in_centrality,
  sum(coalesce(tx_out.from_pagerank, 0) + coalesce(tx_out.from_centrality, 0) +
      coalesce(tx_in.to_pagerank, 0) + coalesce(tx_in.to_centrality, 0)) AS total_influence
RETURN w.address AS whale_address,
       out_pagerank, out_centrality,
       in_pagerank, in_centrality,
       total_influence
ORDER BY total_influence DESC
LIMIT 10
