from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType
from pyspark.sql.functions import col, month, year

# Set Up Spark Config
conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.access.key','AKIATMFNNGPO53WMF6WR')
conf.set('spark.hadoop.fs.s3a.secret.key', '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')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1')
conf.set('spark.jars', '../lib/redshift-jdbc42-2.1.0.26.jar')

# Initialize SparkSession
spark = SparkSession.builder.appName("Monthly Consumption Transformer").config(conf=conf).getOrCreate()

# Define the S3 bucket path containing the CSV files
s3_bucket_path = "s3a://electricity-consumption-master-data/power_consumption_data_202307*.csv"

# Define the CSV files' data schema
schema = StructType([
    StructField("meter_id", IntegerType(), nullable=False),
    StructField("measure", FloatType(), nullable=False),
    StructField("datetime_measured", TimestampType(), nullable=False)
])

# Read CSV files into DataFrame
daily_consumption_df = spark.read.csv(s3_bucket_path, schema=schema, header=True)
daily_consumption_df = daily_consumption_df.withColumn('month', month(col('datetime_measured')))
daily_consumption_df = daily_consumption_df.withColumn('year', year(col('datetime_measured')))

# Redshift Connection Details
redshift_url = "jdbc:redshift://{host}:{port}/{database}".format(
    host="redshift-cluster-1.cbkd07elg7lb.ap-southeast-1.redshift.amazonaws.com",
    port="5439",
    database="dev"
)

redshift_properties = {
    "user": "awsuser",
    "password": "Ktrung1709",
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

# Calculate total consumption daily
monthly_total_consumption_df = daily_consumption_df.groupBy(['month', 'year']).agg({'measure': 'sum'}).withColumnRenamed("sum(measure)", "consumption")
monthly_total_consumption_df.select('month', 'year', 'consumption').show()

# Write the data back to Redshift
monthly_total_consumption_df.write.jdbc(url=redshift_url, table='serving.monthly_total_consumption' , mode='append', properties=redshift_properties)

# Stop SparkSession
spark.stop()