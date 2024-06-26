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
spark = SparkSession.builder.appName("Fact Revenue Residential Transformer").config(conf=conf).getOrCreate()

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
query = "(SELECT f.customer_id, f.contract_id, f.meter_id, d.month, d.year, f.consumption \
            FROM dwh.fact_power_consumption f \
            inner join dwh.dim_date d on f.date_id = d.date_id \
            inner join dwh.dim_customer c on c.customer_id = f.customer_id \
            where c.customer_type = 'residential' and d.month = 10 and d.year = 2023) as tmp"
fact_residential_consumption_df = spark.read.jdbc(redshift_url, query, properties=redshift_properties)

# Add the date_id column
fact_residential_consumption_df = fact_residential_consumption_df.withColumn('day', lit(1))
fact_residential_consumption_df = fact_residential_consumption_df\
    .withColumn('default_date', make_date(col('year'), col('month'), col('day')))
fact_residential_consumption_df = fact_residential_consumption_df\
    .withColumn('date_string', date_format(col('default_date'), "yyyyMMdd"))
fact_residential_consumption_df = fact_residential_consumption_df\
    .withColumn('date_id', col('date_string').cast(IntegerType()))

fact_monthly_residential_consumption_df = fact_residential_consumption_df.groupBy(['customer_id', 'contract_id', 'meter_id', 'date_id'])\
    .agg({'consumption': 'sum'}).withColumnRenamed("sum(consumption)", "consumption")


query = "(select * from cmis.residential_price) as tmp"
residential_price_df = spark.read.jdbc(redshift_url, query, properties=redshift_properties)
residential_price_list = [row.price for row in residential_price_df.select('price').collect()]

fact_residential_revenue_df = fact_monthly_residential_consumption_df\
    .withColumn('revenue', when(col("consumption") <= 50, col("consumption") * residential_price_list[0])
                .when(col("consumption") <= 100, 50 * residential_price_list[0] + (col("consumption") - 50 ) * residential_price_list[1])
                .when(col("consumption") <= 200, 50 * residential_price_list[0] + 50 * residential_price_list[1] + (col("consumption") - 100 ) * residential_price_list[2])
                .when(col("consumption") <= 300, 50 * residential_price_list[0] + 50 * residential_price_list[1] + 100 * residential_price_list[2] + (col("consumption") - 200 ) * residential_price_list[3])
                .when(col("consumption") <= 400, 50 * residential_price_list[0] + 50 * residential_price_list[1] + 100 * residential_price_list[2] + 100 * residential_price_list[3] + (col("consumption") - 300 ) * residential_price_list[4])
                .otherwise(50 * residential_price_list[0] + 50 * residential_price_list[1] + 100 * residential_price_list[2] + 100 * residential_price_list[3] + 100 * residential_price_list[4] + (col("consumption") - 400 ) * residential_price_list[5]))
fact_residential_revenue_df\
    .select('date_id', 'customer_id', 'contract_id', 'meter_id', 'revenue').orderBy('customer_id').show()

# fact_residential_revenue_df\
#     .select('date_id', 'customer_id', 'contract_id', 'meter_id', 'revenue')\
#     .write.jdbc(url=redshift_url, table='dwh.fact_revenue' , mode='append', properties=redshift_properties)


spark.stop()