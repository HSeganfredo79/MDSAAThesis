#!/usr/bin/env python3

import json
import requests
from kafka import KafkaConsumer, KafkaAdminClient, TopicPartition
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from time import sleep
import pandas as pd
from tabulate import tabulate

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "192.168.0.4:9092"
KAFKA_TOPIC = "usdc-transactions"
KAFKA_GROUP_ID = "kafka-consumer"

NEO4J_URI = "bolt://192.168.0.5:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "PotatoDTND12!"

# Initialize Kafka Consumer
def check_kafka():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest", # earliest = first message 
                      #"latest" = ideal for real-time streaming pipelines, newly produced message arriving after 
                      #           the consumer started listening
            enable_auto_commit=True,
        group_id=KAFKA_GROUP_ID	
        )

        print("\n*** 🔍 Checking Kafka for First Incoming Transaction... ***")
        first_message = next(iter(consumer), None)
        if first_message:
            print(f"✅ First Kafka Data Received: {first_message.value}")
        else:
            print("⚠️ No messages found in Kafka.")

        # Now explicitly fetch the last message using poll()
        print("\n *** 🔍 Checking Kafka for Latest Incoming Transactions (per partition)... ***")
        consumer.poll(timeout_ms=1000)  # Initial poll to assign partitions
        consumer.seek_to_end()

        partitions = consumer.assignment()
        for tp in partitions:
            last_offset = consumer.position(tp) - 1
            if last_offset >= 0:
                consumer.seek(tp, last_offset)
                msg_pack = consumer.poll(timeout_ms=1000)
                if msg_pack:
                    for records in msg_pack.values():
                        for record in records:
                            print(f"✅ Latest Kafka Data Received: {record.value}")
                else:
                    print("⚠️ No latest message received.")
            else:
                print("⚠️ Partition empty, no latest message.")
    
    except Exception as e:
        print(f"❌ Error connecting to Kafka: {e}")
        exit(1)

def get_topic_depth():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="aml_pipeline_consumer"
        )

        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)

        total_messages = 0
        for partition in partitions:
            topic_partition = TopicPartition(KAFKA_TOPIC, partition)  # ✅ Correct Usage
            end_offset = consumer.end_offsets([topic_partition])[topic_partition]
            start_offset = consumer.beginning_offsets([topic_partition])[topic_partition]
            total_messages += (end_offset - start_offset)

        print(f"\n📊 Kafka Topic Depth: {total_messages} messages in '{KAFKA_TOPIC}'")

    except Exception as e:
        print(f"❌ Error fetching topic depth: {e}")

# Neo4j verification
def check_neo4j():
    print("\n *** 🔍 Querying Neo4j for oldest and newest transactions: ***")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            queries = {
                # MATCH (from:Wallet)-[:SENT]->(tx:Transaction)-[:RECEIVED]->(to:Wallet)
                "Oldest": """
                    MATCH (tx:Transaction)
                        OPTIONAL MATCH (from:Wallet)-[:SENT]->(tx)
                        OPTIONAL MATCH (tx)-[:RECEIVED]->(to:Wallet)
                    RETURN tx
                    ORDER BY tx.timestamp ASC
                    LIMIT 1
                """,
                "Newest": """
                    MATCH (tx:Transaction)
                        OPTIONAL MATCH (from:Wallet)-[:SENT]->(tx)
                        OPTIONAL MATCH (tx)-[:RECEIVED]->(to:Wallet)
                    RETURN tx
                    ORDER BY tx.timestamp DESC
                    LIMIT 1
                """
            }
            for label, query in queries.items():
                result = session.run(query)
                record = result.single()
                if record:
                    tx = record["tx"]
                    print(f"\n✅ {label} Transaction:")
                    print(f"⏱  Timestamp: {tx['timestamp']}")
                    print(f"🔁  From: {tx['from_address']} ➡ To: {tx['to_address']}")
                    print(f"💰  Amount: {tx.get('amount', 'N/A')}")
                    print(f"📄  Extra: {dict(tx)}")
                else:
                    print(f"⚠️ {label} Transaction: Not found.")

            print("\n *** 🔍 Sample dataframe with the most recent transactions ***")
            query2 = """
                MATCH (tx:Transaction)
                        OPTIONAL MATCH (from:Wallet)-[:SENT]->(tx)
                        OPTIONAL MATCH (tx)-[:RECEIVED]->(to:Wallet)
                    RETURN
                    tx.transaction_id AS transaction_id,
                    tx.amount AS amount,
                    tx.timestamp AS timestamp,
                    tx.from_address AS from_address,
                    tx.from_centrality AS from_centrality,                  
                    tx.from_pagerank AS from_pagerank,
                    tx.to_address AS to_address,
                    tx.to_centrality AS to_centrality,
                    tx.to_pagerank AS to_pagerank,
                    tx.processed_at AS processed_at
                    ORDER BY timestamp DESC
                  LIMIT 10
              """
            # Set options to show all columns and full width
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 0)
            pd.set_option('display.max_colwidth', None)
            pd.set_option('display.float_format', '{:,.8f}'.format)  # 8 decimals
            results = session.run(query2)
            df = pd.DataFrame([r.data() for r in results])           
            #print(tabulate(df, headers='keys', tablefmt='fancy_grid'))
            print(df)

            print("\n *** 🔍 Sample dataframe with the higest pagerank & centrality transactions ***")
            query3 = """
                MATCH (tx:Transaction)
                        OPTIONAL MATCH (from:Wallet)-[:SENT]->(tx)
                        OPTIONAL MATCH (tx)-[:RECEIVED]->(to:Wallet)
                    RETURN
                    tx.transaction_id AS transaction_id,
                    tx.amount AS amount,
                    tx.timestamp AS timestamp,
                    tx.from_address AS from_address,
                    tx.from_centrality AS from_centrality,
                    tx.from_pagerank AS from_pagerank,
                    tx.to_address AS to_address,
                    tx.to_centrality AS to_centrality,
                    tx.to_pagerank AS to_pagerank,
                    tx.processed_at AS processed_at
                    ORDER BY from_centrality DESC, to_centrality DESC, from_pagerank DESC, to_pagerank DESC
                  LIMIT 10
              """
            # Set options to show all columns and full width
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 0)
            pd.set_option('display.max_colwidth', None)
            pd.set_option('display.float_format', '{:,.8f}'.format)  # 8 decimals
            results = session.run(query3)
            df = pd.DataFrame([r.data() for r in results])           
            print(df)
            #print(tabulate(df, headers='keys', tablefmt='fancy_grid'))

            driver.close()
    except Exception as e:
        print(f"❌ Error querying Neo4j: {e}")

# Run all checks
if __name__ == "__main__":
    print("🚨 Starting Pipeline Verification 🚨\n")
    get_topic_depth()
    check_kafka()
    sleep(2)  # Delay for better verification
    check_neo4j()
    print("\n✅ Pipeline Verification Complete!")



