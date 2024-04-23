from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
from pyspark.sql.functions import col, lit

# Set Up Spark Config
conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.access.key','AKIATMFNNGPO53WMF6WR')
conf.set('spark.hadoop.fs.s3a.secret.key', '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')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.1')
conf.set('spark.jars', 'lib/redshift-jdbc42-2.1.0.26.jar')

# Initialize SparkSession
spark = SparkSession.builder.appName("Daily Consumption By Customer Transformer").config(conf=conf).getOrCreate()

# Define the S3 bucket path containing the JSON files
s3_bucket_path = "s3a://electricity-consumption-master-data/power_consumption_data_20230101*.json"

# Define the JSON files' data schema
schema = StructType([
    StructField("meter_id", StringType(), nullable=False),
    StructField("measure", FloatType(), nullable=False),
    StructField("datetime_measured", TimestampType(), nullable=False)
])

# Read JSON files into DataFrame
daily_consumption_df = spark.read.schema(schema).json(s3_bucket_path)
daily_consumption_df = daily_consumption_df.withColumn("meter_id", col("meter_id").cast("int"))

# daily_consumption_df.printSchema()
# daily_consumption_df.show()

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
        inner join cmis.electric_meter m on co.contract_id = m.contract_id ) as tmp"
customer_meter_df = spark.read.jdbc(redshift_url, query, properties=redshift_properties)

# Calculate total consumption by customer
daily_consumption_by_customer_df = daily_consumption_df.join(customer_meter_df, 'meter_id')\
    .groupBy(['customer_id', 'customer_name']).agg({'measure': 'sum'}).withColumnRenamed("sum(measure)", "consumption")
daily_consumption_by_customer_df.select('customer_id', 'customer_name', 'consumption').orderBy('consumption', ascending = False).show()
daily_consumption_by_customer_df = daily_consumption_by_customer_df.withColumn('date', lit('2023-01-01')) # Add a constant date column

# Write the data back to Redshift
daily_consumption_by_customer_df.write.jdbc(url=redshift_url, table='serving.daily_consumption_by_customer' , mode='append', properties=redshift_properties)

# Stop SparkSession
spark.stop()