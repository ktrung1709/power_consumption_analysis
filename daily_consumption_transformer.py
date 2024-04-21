from pyspark.sql import SparkSession
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Daily Consumption Transformer") \
    .getOrCreate()

# Define the directory containing the JSON files
input_dir = "E:/hourly_data"

# Get list of JSON files in the directory
json_files = [os.path.join(input_dir, file) for file in os.listdir(input_dir) if file.endswith(".json") and "20230101" in file]

# Read JSON files into DataFrame
df = spark.read.json(json_files)

# Calculate total consumption by group and show all data
total_consumption = df.groupBy("meter_id").sum("measure").orderBy("meter_id")
total_consumption.show(df.count(), False)

# Stop SparkSession
spark.stop()