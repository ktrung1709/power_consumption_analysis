import psycopg2
import unittest
from datetime import date
from dotenv import load_dotenv
import os

# Connect to Amazon Redshift database
load_dotenv()
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_DBNAME = os.getenv('REDSHIFT_DBNAME')

class TestUpdateDimMeter(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.conn = psycopg2.connect(
            dbname=REDSHIFT_DBNAME,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT
        )
        cls.cursor = cls.conn.cursor()

        # Setup test environment
        cls.cursor.execute("""
            INSERT INTO testing.electric_meter 
            (meter_id, contract_id, meter_type, voltage, status, location, latitude, longitude, iso_code, updated_date) VALUES
            (236, 48, 'single-phase', 'less than 6kV', 'active', 'Kiên Giang1', 9.825, 105.1259,'VN-47', '2024-06-15'),
            (237, 49, 'single-phase', 'less than 6kV', 'active', 'Kiên Giang1', 9.825, 105.1259,'VN-47', '2024-06-15');

            INSERT INTO testing.dim_electric_meter 
            (meter_id, meter_type, voltage, status, location, latitude, longitude, iso_code, updated_date) VALUES
            (236, 'single-phase', 'less than 6kV', 'active', 'Kiên Giang', 9.825, 105.1259,'VN-47', '2024-06-14');
        """)
        cls.conn.commit()

    @classmethod
    def tearDownClass(cls):
        # Clean up
        cls.cursor.execute("""
            TRUNCATE TABLE testing.electric_meter;
            TRUNCATE TABLE testing.dim_electric_meter;
        """)
        cls.conn.commit()
        cls.cursor.close()
        cls.conn.close()

    def test_update_dim_electric_meter(self):
        # Execute the procedure
        self.cursor.execute("CALL testing.update_dim_electric_meter();")
        self.conn.commit()

        # Verify the results
        self.cursor.execute("SELECT meter_id, meter_type, voltage, status, location, latitude, longitude, iso_code, updated_date FROM testing.dim_electric_meter ORDER BY meter_id;")
        results = self.cursor.fetchall()

        expected_results = [
            (236, 'single-phase', 'less than 6kV', 'active', 'Kiên Giang1', 9.825, 105.1259,'VN-47', date(2024, 6, 15)),
            (237, 'single-phase', 'less than 6kV', 'active', 'Kiên Giang1', 9.825, 105.1259,'VN-47', date(2024, 6, 15))
        ]

        self.assertEqual(results, expected_results)

if __name__ == '__main__':
    unittest.main()