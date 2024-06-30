import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

class TestIndustrialRevenueTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestIndustrialRevenueTransformer") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_industrial_revenue_transformation(self):
        # Sample data for fact_power_consumption
        power_consumption_data = [
            (1001, 2001, 1, 10, 2023, 'low', 'less than 6kV', 100.0),
            (1001, 2001, 1, 10, 2023, 'normal', 'less than 6kV', 200.0),
            (1001, 2001, 1, 10, 2023, 'high', 'less than 6kV', 300.0),
            (1001, 2001, 2, 10, 2023, 'low', '6kV to less than 22kV', 100.0),
            (1001, 2001, 2, 10, 2023, 'normal', '6kV to less than 22kV', 200.0),
            (1001, 2001, 2, 10, 2023, 'high', '6kV to less than 22kV', 300.0),
            (1002, 2002, 3, 10, 2023, 'low', '22kV to less than 100kV', 100.0),
            (1002, 2002, 3, 10, 2023, 'normal', '22kV to less than 100kV', 200.0),
            (1002, 2002, 3, 10, 2023, 'high', '22kV to less than 100kV', 300.0),
            (1003, 2003, 4, 10, 2023, 'low', '100kV and above', 100.0),
            (1003, 2003, 4, 10, 2023, 'normal', '100kV and above', 200.0),
            (1003, 2003, 4, 10, 2023, 'high', '100kV and above', 300.0)
        ]

        power_consumption_schema = StructType([
            StructField("customer_id", IntegerType(), nullable=False),
            StructField("contract_id", IntegerType(), nullable=False),
            StructField("meter_id", IntegerType(), nullable=False),
            StructField("month", IntegerType(), nullable=False),
            StructField("year", IntegerType(), nullable=False),
            StructField("time_of_day", StringType(), nullable=False),
            StructField("voltage", StringType(), nullable=False),
            StructField("consumption", FloatType(), nullable=False)
        ])

        fact_industrial_consumption_df = self.spark.createDataFrame(power_consumption_data, power_consumption_schema)

        # Sample data for industrial_price
        price_data = [
            ('less than 6kV', 'normal', 2000.0),
            ('less than 6kV', 'low', 1000.0),
            ('less than 6kV', 'high', 3000.0),
            ('6kV to less than 22kV', 'normal', 5000.0),
            ('6kV to less than 22kV', 'low', 4000.0),
            ('6kV to less than 22kV', 'high', 6000.0),
            ('22kV to less than 100kV', 'normal', 8000.0),
            ('22kV to less than 100kV', 'low', 7000.0),
            ('22kV to less than 100kV', 'high', 9000.0),
            ('100kV and above', 'normal', 10000.0),
            ('100kV and above', 'low', 11000.0),
            ('100kV and above', 'high', 12000.0),
        ]

        price_schema = StructType([
            StructField("voltage_tier", StringType(), nullable=False),
            StructField("time_of_day", StringType(), nullable=False),
            StructField("price", FloatType(), nullable=False)
        ])

        industrial_price_df = self.spark.createDataFrame(price_data, price_schema)

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

        join_cond = [industrial_price_df.voltage_tier == fact_monthly_industrial_consumption_df.voltage, 
                     industrial_price_df.time_of_day == fact_monthly_industrial_consumption_df.time_of_day]

        fact_industrial_revenue_df = fact_monthly_industrial_consumption_df.join(industrial_price_df, join_cond)\
            .withColumn("time_of_day_revenue", fact_monthly_industrial_consumption_df.consumption * industrial_price_df.price)

        fact_industrial_revenue_df = fact_industrial_revenue_df.groupBy(['customer_id', 'contract_id', 'meter_id', 'date_id'])\
            .agg({'time_of_day_revenue': 'sum'}).withColumnRenamed("sum(time_of_day_revenue)", "revenue")

        fact_industrial_revenue_df = fact_industrial_revenue_df\
            .select('date_id', 'customer_id', 'contract_id', 'meter_id', 'revenue')\
            .orderBy('customer_id', 'contract_id', 'meter_id', 'date_id')

        # Expected data
        expected_data = [
            (20231001, 1001, 2001, 1, 1400000.0),  
            (20231001, 1001, 2001, 2, 3200000.0),
            (20231001, 1002, 2002, 3, 5000000.0),
            (20231001, 1003, 2003, 4, 6700000.0)  
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
        self.assertEqual(fact_industrial_revenue_df.collect(), expected_df.collect())

if __name__ == "__main__":
    unittest.main()