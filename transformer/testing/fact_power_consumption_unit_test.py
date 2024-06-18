import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, FloatType, StringType
from pyspark.sql.functions import col, hour, when, date_format
from datetime import datetime

class TestPowerConsumptionTransformer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestPowerConsumptionTransformer") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_daily_consumption_transformation(self):
        # Sample data for power consumption
        power_consumption_data = [
            (1, 5.0, datetime(2023, 7, 1, 10, 0)),
            (2, 3.0, datetime(2023, 7, 1, 18, 0)),
            (3, 2.0, datetime(2023, 7, 1, 23, 0)),
            (4, 1.5, datetime(2023, 7, 1, 2, 0)),
            (1, 4.0, datetime(2023, 7, 2, 10, 0)),
            (2, 3.5, datetime(2023, 7, 2, 18, 0)),
            (3, 2.5, datetime(2023, 7, 2, 23, 0)),
            (4, 1.0, datetime(2023, 7, 2, 2, 0))
        ]

        power_consumption_schema = StructType([
            StructField("meter_id", IntegerType(), nullable=False),
            StructField("measure", FloatType(), nullable=False),
            StructField("datetime_measured", TimestampType(), nullable=False)
        ])

        power_consumption_df = self.spark.createDataFrame(power_consumption_data, power_consumption_schema)

        # Sample data for customer-meter relationship
        customer_meter_data = [
            (1, 1001, 2001),
            (2, 1002, 2002),
            (3, 1003, 2003),
            (4, 1004, 2004)
        ]

        customer_meter_schema = StructType([
            StructField("meter_id", IntegerType(), nullable=False),
            StructField("customer_id", IntegerType(), nullable=False),
            StructField("contract_id", IntegerType(), nullable=False)
        ])

        customer_meter_df = self.spark.createDataFrame(customer_meter_data, customer_meter_schema)

        # Transformations
        daily_consumption_df = power_consumption_df.withColumn('date_string', date_format(col('datetime_measured'), "yyyyMMdd"))
        daily_consumption_df = daily_consumption_df.withColumn('date_id', col('date_string').cast(IntegerType()))
        daily_consumption_df = daily_consumption_df.withColumn('time_of_day', 
            when(((hour(col('datetime_measured')) <= 11) & (hour(col('datetime_measured')) > 9)) | 
                 ((hour(col('datetime_measured')) <= 20) & (hour(col('datetime_measured')) > 17)), 'high')
            .when((hour(col('datetime_measured')) == 23) | 
                  ((hour(col('datetime_measured')) <= 4) & (hour(col('datetime_measured')) >= 0)), 'low')
            .otherwise('normal'))

        fact_consumption_df = daily_consumption_df.join(customer_meter_df, 'meter_id')\
            .groupBy(['customer_id', 'contract_id', 'meter_id', 'date_id', 'time_of_day']).agg({'measure': 'sum'})\
            .withColumnRenamed("sum(measure)", "consumption")

        # Sort the DataFrame
        fact_consumption_df = fact_consumption_df.orderBy('customer_id', 'contract_id', 'meter_id', 'date_id', 'time_of_day')

        # Expected data
        expected_data = [
            (1001, 2001, 1, 20230701, 'high', 5.0),
            (1001, 2001, 1, 20230702, 'high', 4.0),
            (1002, 2002, 2, 20230701, 'high', 3.0),
            (1002, 2002, 2, 20230702, 'high', 3.5),
            (1003, 2003, 3, 20230701, 'low', 2.0),
            (1003, 2003, 3, 20230702, 'low', 2.5),
            (1004, 2004, 4, 20230701, 'low', 1.5),
            (1004, 2004, 4, 20230702, 'low', 1.0)
        ]

        expected_schema = StructType([
            StructField("customer_id", IntegerType(), nullable=False),
            StructField("contract_id", IntegerType(), nullable=False),
            StructField("meter_id", IntegerType(), nullable=False),
            StructField("date_id", IntegerType(), nullable=False),
            StructField("time_of_day", StringType(), nullable=False),
            StructField("consumption", FloatType(), nullable=False)
        ])

        expected_df = self.spark.createDataFrame(expected_data, expected_schema)
        expected_df = expected_df.orderBy('customer_id', 'contract_id', 'meter_id', 'date_id', 'time_of_day')

        # Collect and sort the results for comparison
        self.assertEqual(fact_consumption_df.collect(), expected_df.collect())

if __name__ == "__main__":
    unittest.main()