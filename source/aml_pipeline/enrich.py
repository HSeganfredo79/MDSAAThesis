# --- kafka_enrichment.py (DELAYED ENRICHMENT, WITH GDS FALLBACK + DATE ARG SUPPORT) ---
import logging
import sys
from datetime import datetime, timedelta
from neo4j import GraphDatabase
import networkx as nx

NEO4J_URI = "bolt://192.168.0.5:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "PotatoDTND12!"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def parse_input_date():
    if len(sys.argv) != 2:
        logging.error("❌ Please provide a date in format YYYYMMDD as an argument.")
        sys.exit(1)
    try:
        date = datetime.strptime(sys.argv[1], "%Y%m%d")
        start_ts = int(datetime.combine(date, datetime.min.time()).timestamp())
        #end_ts = int(datetime.combine(date, datetime.max.time()).timestamp())
        end_ts = int((datetime.combine(date, datetime.min.time()) + timedelta(days=1)).timestamp())
        logging.info(f"🔍 Enriching data from {start_ts} to {end_ts} in the graph database...")
        return start_ts, end_ts
    except ValueError:
        logging.error("❌ Invalid date format. Use YYYYMMDD.")
        sys.exit(1)

def enrich_with_networkx(tx, start_ts, end_ts):
    graph_start = start_ts - 86400 * 7  # 7-day context (previous days)
    query = """
        MATCH (w1:Wallet)-[:SENT]->(tx:Transaction)-[:RECEIVED_BY]->(w2:Wallet)
        WHERE tx.timestamp >= $graph_start AND tx.timestamp <= $end_ts
        RETURN w1.address AS from_address, w2.address AS to_address
    """
    results = tx.run(query, graph_start=graph_start, end_ts=end_ts)

    G = nx.DiGraph()
    for record in results:
        G.add_edge(record["from_address"], record["to_address"])

    logging.info(f"🔍 Extracted {G.number_of_nodes()} wallets and {G.number_of_edges()} edges from Neo4j")
    return nx.pagerank(G), nx.degree_centrality(G)

def apply_enrichment(tx, pagerank, centrality, start_ts, end_ts):
    query = """
        MATCH (from_wallet:Wallet)-[:SENT]->(tx:Transaction)-[:RECEIVED_BY]->(to_wallet:Wallet)
        WHERE tx.consumed_at IS NOT NULL AND tx.timestamp >= $start_ts AND tx.timestamp < $end_ts
        WITH tx, from_wallet, to_wallet,
             coalesce($pagerank[from_wallet.address], 0.0) AS fp,
             coalesce($pagerank[to_wallet.address], 0.0) AS tp,
             coalesce($centrality[from_wallet.address], 0.0) AS fc,
             coalesce($centrality[to_wallet.address], 0.0) AS tc
        SET tx.from_pagerank = fp,
            tx.to_pagerank = tp,
            tx.from_centrality = fc,
            tx.to_centrality = tc,
            //from_wallet.pagerank = fp,
            //from_wallet.centrality = fc,
            //to_wallet.pagerank = tp,
            //to_wallet.centrality = tc,
            tx.enriched_at = timestamp()
        RETURN count(tx) AS enriched_count
    """
    result = tx.run(query, pagerank=pagerank, centrality=centrality, start_ts=start_ts, end_ts=end_ts)
    count = result.single()["enriched_count"]
    logging.info(f"✅ Enrichment applied to {count} transactions.")

def run_enrichment():
    start_ts, end_ts = parse_input_date()
    with driver.session() as session:
        pagerank, centrality = session.read_transaction(enrich_with_networkx, start_ts, end_ts)
        #print(pagerank)
        #print(centrality)
        if not pagerank or not centrality:
            logging.warning("⚠️ No data to enrich — empty graph. Skipping update.")
            return
        session.write_transaction(apply_enrichment, pagerank, centrality, start_ts, end_ts)

if __name__ == "__main__":
    run_enrichment()
