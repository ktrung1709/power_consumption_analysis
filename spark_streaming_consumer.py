from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .getOrCreate()

# Define Kafka source properties
kafka_bootstrap_servers = "localhost:29092,localhost:39092"
kafka_topic = "power_consumption_topic"

# Define Kafka source options
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic,
    "startingOffsets": "earliest"  # Optional: Start from the earliest available offset
}

# Read from Kafka source using structured streaming API
streaming_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .load()

# Print streaming data to the console
query = streaming_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


# Wait for the termination of the query
query.awaitTermination()

# Stop Spark session
spark.stop()
