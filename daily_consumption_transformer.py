from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
from pyspark.sql.functions import col

conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.access.key','AKIATMFNNGPO53WMF6WR')
conf.set('spark.hadoop.fs.s3a.secret.key', '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')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1')
# Initialize SparkSession
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Define the S3 bucket path containing the JSON files
s3_bucket_path = "s3a://electricity-consumption-master-data/power_consumption_data_20230101*.json"

schema = StructType([
    StructField("meter_id", StringType(), nullable=False),
    StructField("measure", FloatType(), nullable=False),
    StructField("datetime_measured", TimestampType(), nullable=False)
])

# Read JSON files into DataFrame
df = spark.read.schema(schema).json(s3_bucket_path)
df.printSchema()
df.show()

df = df.withColumn("meter_id", col("meter_id").cast("int"))

df.printSchema()
df.show()
# Calculate total consumption by group and show all data
total_consumption = df.groupBy("meter_id").sum("measure").orderBy("meter_id")
total_consumption.show()

# Stop SparkSession
spark.stop()