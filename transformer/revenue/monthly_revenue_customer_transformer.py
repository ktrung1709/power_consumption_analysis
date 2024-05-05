from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType
from pyspark.sql.functions import col, year, month, when
# Set Up Spark Config
conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.access.key','AKIATMFNNGPO53WMF6WR')
conf.set('spark.hadoop.fs.s3a.secret.key', '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')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1')
conf.set('spark.jars', '../lib/redshift-jdbc42-2.1.0.26.jar')

# Initialize SparkSession
spark = SparkSession.builder.appName("Monthly Revenue By Customer Transformer").config(conf=conf).getOrCreate()

# Define the S3 bucket path containing the CSV files
s3_bucket_path = "s3a://electricity-consumption-master-data/power_consumption_data_20230701*.csv"

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

# Run the query against the Redshift Cluster
query = "(select c.customer_id, c.customer_name, m.meter_id from cmis.customer c \
        inner join cmis.contract co on c.customer_id = co.customer_id \
        inner join cmis.electric_meter m on co.contract_id = m.contract_id \
        where c.customer_type = 'residential' ) as tmp"
residential_customer_meter_df = spark.read.jdbc(redshift_url, query, properties=redshift_properties)

# Calculate total revenue by residential customer
monthly_consumption_by_residential_customer_df = daily_consumption_df.join(residential_customer_meter_df, 'meter_id')\
    .groupBy(['customer_id', 'customer_name', 'year', 'month']).agg({'measure': 'sum'}).withColumnRenamed("sum(measure)", "consumption")
monthly_consumption_by_residential_customer_df.select('customer_id', 'customer_name', 'consumption', 'year', 'month').show()
monthly_consumption_by_residential_customer_df.withColumn("tier", when )


query = "(select * from cmis.residential_price) as tmp"
residential_price_df = spark.read.jdbc(redshift_url, query, properties=redshift_properties)
residential_price_list = [row.price for row in residential_price_df.select('price').collect()]

# Define a function to calculate revenue based on consumption and price tiers
def calculate_residential_revenue(consumption):
    revenue = 0
    if 0 < consumption <= 50:
        revenue = consumption * residential_price_list[0]
    elif consumption <= 100:
        revenue = 50 * residential_price_list[0] + (consumption - 50) * residential_price_list[1]
    elif consumption <= 200:
        revenue = 50 * residential_price_list[0] + 50 * residential_price_list[1] + (consumption - 100) * residential_price_list[2]
    elif consumption <= 300:
        revenue = 50 * residential_price_list[0] + 50 * residential_price_list[1] + 100 * residential_price_list[2] + (consumption - 200) * residential_price_list[3]
    elif consumption <= 400:
        revenue = 50 * residential_price_list[0] + 50 * residential_price_list[1] + 100 * residential_price_list[2] + 100 * residential_price_list[3] + (consumption - 300) * residential_price_list[4]
    else:
        revenue = 50 * residential_price_list[0] + 50 * residential_price_list[1] + 100 * residential_price_list[2] + 100 * residential_price_list[3] + 100 * residential_price_list[4] + (consumption - 400) * residential_price_list[5]
    return revenue

calculate_revenue_residential_udf = spark.udf.register('calculate_revenue_residential_udf', calculate_residential_revenue)
monthly_revenue_by_residential_customer_df = monthly_consumption_by_residential_customer_df \
    .withColumn('revenue', calculate_revenue_residential_udf(monthly_consumption_by_residential_customer_df.consumption))
monthly_revenue_by_residential_customer_df.select('customer_id','customer_name','year', 'month', 'revenue').show()
# Write the data back to Redshift
# monthly_consumption_by_customer_df.write.jdbc(url=redshift_url, table='serving.monthly_consumption_by_customer' , mode='append', properties=redshift_properties)

# Stop SparkSession
spark.stop()