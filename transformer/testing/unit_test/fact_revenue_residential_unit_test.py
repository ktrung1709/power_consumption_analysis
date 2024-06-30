import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

class TestResidentialRevenueTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestResidentialRevenueTransformer") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_residential_revenue_transformation(self):
        # Sample data for fact_power_consumption
        power_consumption_data = [
            (1001, 2001, 1, 7, 2023, 100.0),
            (1002, 2002, 2, 7, 2023, 60.0),
            (1003, 2003, 3, 7, 2023, 150.0),
            (1004, 2004, 4, 7, 2023, 300.0)
        ]

        power_consumption_schema = StructType([
            StructField("customer_id", IntegerType(), nullable=False),
            StructField("contract_id", IntegerType(), nullable=False),
            StructField("meter_id", IntegerType(), nullable=False),
            StructField("month", IntegerType(), nullable=False),
            StructField("year", IntegerType(), nullable=False),
            StructField("consumption", FloatType(), nullable=False)
        ])

        fact_residential_consumption_df = self.spark.createDataFrame(power_consumption_data, power_consumption_schema)

        # Sample data for residential_price
        price_data = [
            (1, 1000.0),
            (2, 2000.0),
            (3, 3000.0),
            (4, 4000.0),
            (5, 5000.0),
            (6, 6000.0)
        ]

        price_schema = StructType([
            StructField("tier", IntegerType(), nullable=False),
            StructField("price", FloatType(), nullable=False)
        ])

        price_df = self.spark.createDataFrame(price_data, price_schema)

        fact_residential_consumption_df = fact_residential_consumption_df.withColumn('day', lit(1))
        fact_residential_consumption_df = fact_residential_consumption_df\
            .withColumn('default_date', make_date(col('year'), col('month'), col('day')))
        fact_residential_consumption_df = fact_residential_consumption_df\
            .withColumn('date_string', date_format(col('default_date'), "yyyyMMdd"))
        fact_residential_consumption_df = fact_residential_consumption_df\
            .withColumn('date_id', col('date_string').cast(IntegerType()))

        fact_monthly_residential_consumption_df = fact_residential_consumption_df.groupBy(['customer_id', 'contract_id', 'meter_id', 'date_id'])\
            .agg({'consumption': 'sum'}).withColumnRenamed("sum(consumption)", "consumption")

        residential_price_list = [row.price for row in price_df.select('price').collect()]

        fact_residential_revenue_df = fact_monthly_residential_consumption_df\
            .withColumn('revenue', when(col("consumption") <= 50, col("consumption") * residential_price_list[0])
                        .when(col("consumption") <= 100, 50 * residential_price_list[0] + (col("consumption") - 50 ) * residential_price_list[1])
                        .when(col("consumption") <= 200, 50 * residential_price_list[0] + 50 * residential_price_list[1] + (col("consumption") - 100 ) * residential_price_list[2])
                        .when(col("consumption") <= 300, 50 * residential_price_list[0] + 50 * residential_price_list[1] + 100 * residential_price_list[2] + (col("consumption") - 200 ) * residential_price_list[3])
                        .when(col("consumption") <= 400, 50 * residential_price_list[0] + 50 * residential_price_list[1] + 100 * residential_price_list[2] + 100 * residential_price_list[3] + (col("consumption") - 300 ) * residential_price_list[4])
                        .otherwise(50 * residential_price_list[0] + 50 * residential_price_list[1] + 100 * residential_price_list[2] + 100 * residential_price_list[3] + 100 * residential_price_list[4] + (col("consumption") - 400 ) * residential_price_list[5]))

        fact_residential_revenue_df = fact_residential_revenue_df \
            .select('date_id', 'customer_id', 'contract_id', 'meter_id', 'revenue') \
            .orderBy('customer_id', 'contract_id', 'meter_id', 'date_id')

        # Expected data
        expected_data = [
            (20230701, 1001, 2001, 1, 150000.0),
            (20230701, 1002, 2002, 2, 70000.0), 
            (20230701, 1003, 2003, 3, 300000.0), 
            (20230701, 1004, 2004, 4, 850000.0) 
        ]

        expected_schema = StructType([
            StructField("date_id", IntegerType(), nullable=False),
            StructField("customer_id", IntegerType(), nullable=False),
            StructField("contract_id", IntegerType(), nullable=False),
            StructField("meter_id", IntegerType(), nullable=False),
            StructField("revenue", FloatType(), nullable=False)
        ])

        expected_df = self.spark.createDataFrame(expected_data, expected_schema)
        expected_df = expected_df.orderBy('customer_id', 'contract_id', 'meter_id', 'date_id')

        # Collect and sort the results for comparison
        self.assertEqual(fact_residential_revenue_df.collect(), expected_df.collect())

if __name__ == "__main__":
    unittest.main()
