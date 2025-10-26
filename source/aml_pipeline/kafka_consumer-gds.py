import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from neo4j import GraphDatabase

# Configuration
KAFKA_BOOTSTRAP_SERVERS = '192.168.0.4:9092'
KAFKA_TOPIC = 'usdc-transactions'
KAFKA_GROUP_ID = 'aml_pipeline_consumer'
NEO4J_URI = 'bolt://192.168.0.5:7687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'PotatoDTND12!'

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=KAFKA_GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    enable_auto_commit=True,
    auto_offset_reset='earliest'
)

# Neo4j driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Create GDS projection and write features
def update_wallet_features(tx):
    tx.run("""
        CALL gds.graph.project(
            'walletGraph',
            'Wallet',
            {
                SENT: {orientation: 'UNDIRECTED'}
            }
        )
    """)

    tx.run("""
        CALL gds.pageRank.write(
            'walletGraph',
            {
                maxIterations: 20,
                dampingFactor: 0.85,
                writeProperty: 'pagerank'
            }
        )
    """)

    tx.run("""
        CALL gds.degree.write(
            'walletGraph',
            {
                writeProperty: 'centrality'
            }
        )
    """)

    tx.run("CALL gds.graph.drop('walletGraph')")

# Store wallet features in the transaction node
def update_transaction_features(tx, txid, from_addr, to_addr):
    tx.run("""
        MATCH (from:Wallet {address: $from_addr})
        MATCH (to:Wallet {address: $to_addr})
        MATCH (t:Transaction {transaction_id: $txid})
        SET t.from_pagerank = coalesce(from.pagerank, 0.0),
            t.to_pagerank = coalesce(to.pagerank, 0.0),
            t.from_centrality = coalesce(from.centrality, 0.0),
            t.to_centrality = coalesce(to.centrality, 0.0)
    """, txid=txid, from_addr=from_addr, to_addr=to_addr)

# Main transaction handling
def process_transaction(tx, data):
    txid = data['transaction_id']
    from_addr = data['from_address']
    to_addr = data['to_address']
    amount = float(data['amount'])
    timestamp = int(data['timestamp'])
    block_number = data['block_number']
    processed_at = int(datetime.utcnow().timestamp())

    # Insert nodes and relationships
    tx.run("MERGE (:Wallet {address: $address})", address=from_addr)
    tx.run("MERGE (:Wallet {address: $address})", address=to_addr)

    tx.run("""
        MERGE (tx:Transaction {transaction_id: $txid})
        SET tx.block_number = $block_number,
            tx.amount = $amount,
            tx.timestamp = $timestamp,
            tx.from_address = $from_addr,
            tx.to_address = $to_addr,
            tx.processed_at = $processed_at
        WITH tx
        MATCH (from:Wallet {address: $from_addr})
        MATCH (to:Wallet {address: $to_addr})
        MERGE (from)-[:SENT]->(tx)
        MERGE (tx)-[:RECEIVED]->(to)
    """, txid=txid, block_number=block_number, amount=amount, timestamp=timestamp,
         from_addr=from_addr, to_addr=to_addr, processed_at=processed_at)

    update_wallet_features(tx)
    update_transaction_features(tx, txid, from_addr, to_addr)

# Main loop
def consume():
    logging.info("🟢 Kafka consumer started...")
    for message in consumer:
        try:
            data = message.value
            with driver.session() as session:
                session.write_transaction(process_transaction, data)
            logging.info(f"✅ Processed transaction {data['transaction_id']}")
        except Exception as e:
            logging.error(f"❌ Error processing transaction: {e}")

if __name__ == '__main__':
    consume()
