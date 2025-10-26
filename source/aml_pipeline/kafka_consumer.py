# --- kafka_consumer.py (FAST INGESTION ONLY) ---
import json
import logging
import multiprocessing
from confluent_kafka import Consumer, KafkaException
from neo4j import GraphDatabase
from datetime import datetime

# Configuration
KAFKA_TOPIC = "usdc-transactions"
KAFKA_BROKER = "192.168.0.4:9092"
KAFKA_GROUP = "aml_pipeline_consumer"
NEO4J_URI = "bolt://192.168.0.5:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "PotatoDTND12!"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def _write_tx(tx, data):
    tx_id = data["transaction_id"]
    from_addr = data["from_address"]
    to_addr = data["to_address"]

    data.update({
        "consumed_at": int(datetime.utcnow().timestamp())
    })

    tx.run("""
        MERGE (tx:Transaction {transaction_id: $transaction_id})
        SET tx += $tx_data

        MERGE (from_wallet:Wallet {address: $from_address})
        MERGE (to_wallet:Wallet {address: $to_address})

        MERGE (from_wallet)-[:SENT]->(tx)
        MERGE (tx)-[:RECEIVED_BY]->(to_wallet)
    """, transaction_id=tx_id, tx_data=data, from_address=from_addr, to_address=to_addr)

def process_transaction(data):
    with driver.session() as session:
        session.write_transaction(_write_tx, data)

def worker_process(worker_id):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 6000
    })
    consumer.subscribe([KAFKA_TOPIC])

    logging.info(f"👷 Worker {worker_id} started")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            data = json.loads(msg.value().decode("utf-8"))
            process_transaction(data)
            consumer.commit()
            logging.info(f"✅ Worker {worker_id} processed: {data['transaction_id']}")
    finally:
        consumer.close()

if __name__ == "__main__":
    num_workers = 8
    pool = multiprocessing.Pool(processes=num_workers)
    pool.map(worker_process, range(num_workers))
