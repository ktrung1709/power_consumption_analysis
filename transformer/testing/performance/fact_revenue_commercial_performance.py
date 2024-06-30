from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *
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
spark = SparkSession.builder.appName("Fact Revenue Commercial Transformer").config(conf=conf).getOrCreate()

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
query = "(SELECT f.customer_id, f.contract_id, f.meter_id, d.month, d.year, f.time_of_day, m.voltage, f.consumption \
            FROM dwh.fact_power_consumption f \
            inner join dwh.dim_date d on f.date_id = d.date_id \
            inner join dwh.dim_customer c on c.customer_id = f.customer_id \
            inner join dwh.dim_electric_meter m on m.meter_id = f.meter_id \
            where c.customer_type = 'commercial' and d.month = 10 and d.year = 2023) as tmp"
fact_commercial_consumption_df = spark.read.jdbc(redshift_url, query, properties=redshift_properties)

# Add the date_id column
fact_commercial_consumption_df = fact_commercial_consumption_df.withColumn('day', lit(1))
fact_commercial_consumption_df = fact_commercial_consumption_df\
    .withColumn('default_date', make_date(col('year'), col('month'), col('day')))
fact_commercial_consumption_df = fact_commercial_consumption_df\
    .withColumn('date_string', date_format(col('default_date'), "yyyyMMdd"))
fact_commercial_consumption_df = fact_commercial_consumption_df\
    .withColumn('date_id', col('date_string').cast(IntegerType()))

fact_monthly_commercial_consumption_df = fact_commercial_consumption_df\
    .groupBy(['customer_id', 'contract_id', 'meter_id', 'date_id', 'time_of_day', 'voltage'])\
    .agg({'consumption': 'sum'}).withColumnRenamed("sum(consumption)", "consumption")

query = "(select * from cmis.commercial_price) as tmp"
commercial_price_df = spark.read.jdbc(redshift_url, query, properties=redshift_properties)

join_cond = [commercial_price_df.voltage_tier == fact_monthly_commercial_consumption_df.voltage, \
        commercial_price_df.time_of_day == fact_monthly_commercial_consumption_df.time_of_day]

fact_commercial_revenue_df = fact_monthly_commercial_consumption_df.join(commercial_price_df, join_cond)\
    .withColumn("time_of_day_revenue", fact_monthly_commercial_consumption_df.consumption * commercial_price_df.price)

fact_commercial_revenue_df = fact_commercial_revenue_df.groupBy(['customer_id', 'contract_id', 'meter_id', 'date_id'])\
    .agg({'time_of_day_revenue': 'sum'}).withColumnRenamed("sum(time_of_day_revenue)", "revenue")\

fact_commercial_revenue_df\
    .select('date_id', 'customer_id', 'contract_id', 'meter_id', 'revenue').show()

# fact_commercial_revenue_df\
#     .select('date_id', 'customer_id', 'contract_id', 'meter_id', 'revenue')\
#     .write.jdbc(url=redshift_url, table='dwh.fact_revenue' , mode='append', properties=redshift_properties)

spark.stop()