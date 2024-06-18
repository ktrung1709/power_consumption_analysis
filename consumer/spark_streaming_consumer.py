from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Spark Streaming Consumer") \
    .config("spark.jars", "lib/postgresql-42.7.3.jar") \
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

# Define the schema for the CSV data
schema = StructType([
    StructField("meter_id", IntegerType(), True),
    StructField("measure", DoubleType(), True),
    StructField("datetime_measured", TimestampType(), True)
])

# Convert the Kafka message value from binary to string and parse CSV
parsed_df = streaming_df.selectExpr("CAST(value AS STRING) as csv_value") \
    .select(
        split(col("csv_value"), ",").getItem(0).cast(IntegerType()).alias("meter_id"),
        split(col("csv_value"), ",").getItem(1).cast(DoubleType()).alias("measure"),
        split(col("csv_value"), ",").getItem(2).cast(TimestampType()).alias("datetime_measured")
    )

# Define TimescaleDB connection properties
timescale_db_url = "jdbc:postgresql://localhost:5432/speed_layer_db"
timescale_db_properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Function to write data to TimescaleDB
def write_to_timescaledb(batch_df):
    batch_df.write \
        .jdbc(url=timescale_db_url, table="power_consumption_streaming", mode="append", properties=timescale_db_properties)

# Write the parsed data to TimescaleDB
query = parsed_df.writeStream \
    .foreachBatch(write_to_timescaledb) \
    .outputMode("append") \
    .start()

# Wait for the termination of the query
query.awaitTermination()

# Stop Spark session
spark.stop()

