from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Set Up Spark Config
conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.access.key','AKIATMFNNGPO53WMF6WR')
conf.set('spark.hadoop.fs.s3a.secret.key', '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')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1')
conf.set('spark.jars', '../lib/redshift-jdbc42-2.1.0.26.jar')

# Initialize SparkSession
spark = SparkSession.builder.appName("Fact Revenue Industrial Transformer").config(conf=conf).getOrCreate()

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
query = "(SELECT f.customer_id, f.contract_id, f.meter_id, d.month, d.year, f.time_of_day, m.voltage, f.consumption \
            FROM dwh.fact_power_consumption f \
            inner join dwh.dim_date d on f.date_id = d.date_id \
            inner join dwh.dim_customer c on c.customer_id = f.customer_id \
            inner join dwh.dim_electric_meter m on m.meter_id = f.meter_id \
            where c.customer_type = 'industrial' and d.month = 10 and d.year = 2023) as tmp"
fact_industrial_consumption_df = spark.read.jdbc(redshift_url, query, properties=redshift_properties)

# Add the date_id column
fact_industrial_consumption_df = fact_industrial_consumption_df.withColumn('day', lit(1))
fact_industrial_consumption_df = fact_industrial_consumption_df\
    .withColumn('default_date', make_date(col('year'), col('month'), col('day')))
fact_industrial_consumption_df = fact_industrial_consumption_df\
    .withColumn('date_string', date_format(col('default_date'), "yyyyMMdd"))
fact_industrial_consumption_df = fact_industrial_consumption_df\
    .withColumn('date_id', col('date_string').cast(IntegerType()))

fact_monthly_industrial_consumption_df = fact_industrial_consumption_df\
    .groupBy(['customer_id', 'contract_id', 'meter_id', 'date_id', 'time_of_day', 'voltage'])\
    .agg({'consumption': 'sum'}).withColumnRenamed("sum(consumption)", "consumption")

query = "(select * from cmis.industrial_price) as tmp"
industrial_price_df = spark.read.jdbc(redshift_url, query, properties=redshift_properties)

join_cond = [industrial_price_df.voltage_tier == fact_monthly_industrial_consumption_df.voltage, \
        industrial_price_df.time_of_day == fact_monthly_industrial_consumption_df.time_of_day]

fact_industrial_revenue_df = fact_monthly_industrial_consumption_df.join(industrial_price_df, join_cond)\
    .withColumn("time_of_day_revenue", fact_monthly_industrial_consumption_df.consumption * industrial_price_df.price)

fact_industrial_revenue_df = fact_industrial_revenue_df.groupBy(['customer_id', 'contract_id', 'meter_id', 'date_id'])\
    .agg({'time_of_day_revenue': 'sum'}).withColumnRenamed("sum(time_of_day_revenue)", "revenue")\

fact_industrial_revenue_df\
    .select('date_id', 'customer_id', 'contract_id', 'meter_id', 'revenue').show()

fact_industrial_revenue_df\
    .select('date_id', 'customer_id', 'contract_id', 'meter_id', 'revenue')\
    .write.jdbc(url=redshift_url, table='dwh.fact_revenue' , mode='append', properties=redshift_properties)

spark.stop()