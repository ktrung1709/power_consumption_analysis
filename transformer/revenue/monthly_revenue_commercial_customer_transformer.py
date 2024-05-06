from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType
from pyspark.sql.functions import col, year, month, hour, when
# Set Up Spark Config
conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.access.key','AKIATMFNNGPO53WMF6WR')
conf.set('spark.hadoop.fs.s3a.secret.key', '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')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1')
conf.set('spark.jars', '../lib/redshift-jdbc42-2.1.0.26.jar')

# Initialize SparkSession
spark = SparkSession.builder.appName("Monthly Revenue By Commercial Customer Transformer").config(conf=conf).getOrCreate()

# Define the S3 bucket path containing the CSV files
s3_bucket_path = "s3a://electricity-consumption-master-data/power_consumption_data_2023070*.csv"

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
daily_consumption_df = daily_consumption_df.withColumn('time_of_day', 
    when(((hour(col('datetime_measured')) <= 11) & (hour(col('datetime_measured')) > 9)) | 
         ((hour(col('datetime_measured')) <= 20) & (hour(col('datetime_measured')) > 17)), 'high')
    .when((hour(col('datetime_measured')) == 23) | 
          ((hour(col('datetime_measured')) <= 4) & (hour(col('datetime_measured')) >= 0)), 'low')
    .otherwise('normal'))
daily_consumption_df.show()
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
query = "(select c.customer_id, c.customer_name, m.meter_id, m.voltage from cmis.customer c \
        inner join cmis.contract co on c.customer_id = co.customer_id \
        inner join cmis.electric_meter m on co.contract_id = m.contract_id \
        where c.customer_type = 'commercial' ) as tmp"
commercial_customer_meter_df = spark.read.jdbc(redshift_url, query, properties=redshift_properties)


monthly_consumption_by_commercial_customer_df = commercial_customer_meter_df.join(daily_consumption_df, 'meter_id')\
    .groupBy(['customer_id', 'customer_name', 'year', 'month', 'time_of_day', 'voltage']).agg({'measure': 'sum'}).withColumnRenamed("sum(measure)", "consumption")

query = "(select * from cmis.commercial_price) as tmp"
commercial_price_df = spark.read.jdbc(redshift_url, query, properties=redshift_properties)

cond = [commercial_price_df.voltage_tier == monthly_consumption_by_commercial_customer_df.voltage, \
        commercial_price_df.time_of_day == monthly_consumption_by_commercial_customer_df.time_of_day]
monthly_revenue_small_by_commercial_customer_df = monthly_consumption_by_commercial_customer_df.join(commercial_price_df, cond) \
    .withColumn("revenue", monthly_consumption_by_commercial_customer_df.consumption * commercial_price_df.price)
monthly_revenue_by_commercial_customer_df = monthly_revenue_small_by_commercial_customer_df.groupBy(['customer_id', 'customer_name', 'year', 'month'])\
    .agg({'revenue': 'sum'}).withColumnRenamed("sum(revenue)", "revenue")\
    .select('customer_id', 'customer_name', 'year', 'month', 'revenue')
monthly_revenue_by_commercial_customer_df.write.jdbc(url=redshift_url, table='serving.monthly_revenue_by_customer' , mode='append', properties=redshift_properties)
# Stop SparkSession
spark.stop()