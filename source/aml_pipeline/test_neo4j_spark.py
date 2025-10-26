from pyspark.sql import SparkSession

# Initialize Spark with the Neo4j connector JAR
spark = SparkSession.builder \
    .appName("Neo4j Spark Test") \
    .config("spark.jars", "/root/tese_henrique/aml_pipeline/lib/neo4j-connector-apache-spark_2.12-4.1.4_for_spark_3.jar") \
    .getOrCreate()

print("✅ Spark session created.")

try:
    # Run Cypher query wrapped in CALL {} to allow LIMIT
    df = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://192.168.0.5:7687") \
        .option("authentication.type", "basic") \
        .option("authentication.basic.username", "neo4j") \
        .option("authentication.basic.password", "PotatoDTND12!") \
        .option("query", """
            CALL {
              MATCH (n:Wallet)
              RETURN n.address AS address
              LIMIT 5
            }
            RETURN address
        """) \
        .load()

    print("✅ Query executed, displaying result:")
    df.show()

except Exception as e:
    print("❌ Error reading from Neo4j via Spark connector:")
    print(e)
