from neo4j import GraphDatabase
import sys

NEO4J_URI = "bolt://192.168.0.5:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "PotatoDTND12!"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def clear_sent_relationships(tx):
    tx.run("MATCH ()-[r:SENT]->() DELETE r")

def full_reset(tx):
    tx.run("MATCH (n) DETACH DELETE n")

def run_cleanup(mode="sent-only"):
    with driver.session() as session:
        if mode == "sent-only":
            session.execute_write(clear_sent_relationships)
            print("✅ Removed all :SENT relationships. Wallet nodes preserved.")
        elif mode == "full-reset":
            session.execute_write(full_reset)
            print("⚠️  Performed full database reset. All nodes and relationships deleted.")
        else:
            print("❌ Invalid mode. Use 'sent-only' or 'full-reset'.")
    driver.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 cleanup_neo4j.py [sent-only|full-reset]")
        sys.exit(1)
    run_cleanup(sys.argv[1])
