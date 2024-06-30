from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType
from pyspark.sql.functions import col, hour, when, date_format
from dotenv import load_dotenv
import os

# AWS S3 configuration
load_dotenv()
bucket_name = os.getenv('S3_BUCKET_NAME')
access_key_id = os.getenv('S3_ACCESS_KEY_ID')
secret_access_key = os.getenv('S3_SECRET_ACCESS_KEY')

# AWS Redshift configuration
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_DBNAME = os.getenv('REDSHIFT_DBNAME')

# Set Up Spark Config
conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.access.key',access_key_id)
conf.set('spark.hadoop.fs.s3a.secret.key', secret_access_key)
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1')
conf.set('spark.jars', '../lib/redshift-jdbc42-2.1.0.26.jar')

# Initialize SparkSession
spark = SparkSession.builder.appName("Fact Power Consumption Transformer").config(conf=conf).getOrCreate()

# Define the S3 bucket path containing the CSV files
s3_bucket_path = f"s3a://{bucket_name}/power_consumption_data_20230701*.csv"

# Define the CSV files' data schema
schema = StructType([
    StructField("meter_id", IntegerType(), nullable=False),
    StructField("measure", FloatType(), nullable=False),
    StructField("datetime_measured", TimestampType(), nullable=False)
])

# Read CSV files into DataFrame
daily_consumption_df = spark.read.csv(s3_bucket_path, schema=schema, header=True)
daily_consumption_df = daily_consumption_df.withColumn('date_string', date_format(col('datetime_measured'), "yyyyMMdd"))
daily_consumption_df = daily_consumption_df.withColumn('date_id', col('date_string').cast(IntegerType()))
daily_consumption_df = daily_consumption_df.withColumn('time_of_day', 
    when(((hour(col('datetime_measured')) <= 11) & (hour(col('datetime_measured')) > 9)) | 
         ((hour(col('datetime_measured')) <= 20) & (hour(col('datetime_measured')) > 17)), 'high')
    .when((hour(col('datetime_measured')) == 23) | 
          ((hour(col('datetime_measured')) <= 4) & (hour(col('datetime_measured')) >= 0)), 'low')
    .otherwise('normal'))

# Redshift Connection Details
redshift_url = "jdbc:redshift://{host}:{port}/{database}".format(
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    database=REDSHIFT_DBNAME
)

redshift_properties = {
    "user": REDSHIFT_USER,
    "password": REDSHIFT_PASSWORD,
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

# Run the query against the Redshift Cluster
query = "(select c.customer_id, co.contract_id, m.meter_id from cmis.customer c \
        inner join cmis.contract co on c.customer_id = co.customer_id \
        inner join cmis.electric_meter m on co.contract_id = m.contract_id ) as tmp"
customer_meter_df = spark.read.jdbc(redshift_url, query, properties=redshift_properties)

# Calculate fact consumption
fact_consumption_df = daily_consumption_df.join(customer_meter_df, 'meter_id')\
    .groupBy(['customer_id', 'contract_id', 'meter_id', 'date_id', 'time_of_day']).agg({'measure': 'sum'}).withColumnRenamed("sum(measure)", "consumption")
fact_consumption_df.select('date_id', 'customer_id', 'contract_id', 'meter_id', 'time_of_day', 'consumption').show()

# Write the data back to Redshift
# fact_consumption_df.write.jdbc(url=redshift_url, table='dwh.fact_power_consumption' , mode='append', properties=redshift_properties)

# Stop SparkSession
spark.stop()