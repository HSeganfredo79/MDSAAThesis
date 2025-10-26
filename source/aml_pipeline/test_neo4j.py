from neo4j import GraphDatabase

uri = "bolt://192.168.0.5:7687"
user = "neo4j"
password = "PotatoDTND12!"  # Replace with your actual password

driver = GraphDatabase.driver(uri, auth=(user, password))

def check_gds_version(tx):
    result = tx.run("RETURN gds.version() AS version")
    for row in result:
        print("✅ GDS version:", row["version"])

with driver.session() as session:
    session.read_transaction(check_gds_version)
