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

class TestUpdateDimContract(unittest.TestCase):
    
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
            INSERT INTO testing.contract (contract_id, customer_id, date_created, status, updated_date) VALUES
            (1, 1, '2024-06-15', 'active', '2024-06-15'),
            (2, 2, '2024-06-15', 'active', '2024-06-15');

            INSERT INTO testing.dim_contract (contract_id, date_created, status, updated_date) VALUES
            (1, '2024-06-14', 'active', '2024-06-14');
        """)
        cls.conn.commit()

    @classmethod
    def tearDownClass(cls):
        # Clean up
        cls.cursor.execute("""
            TRUNCATE TABLE testing.contract;
            TRUNCATE TABLE testing.dim_contract;
        """)
        cls.conn.commit()
        cls.cursor.close()
        cls.conn.close()

    def test_update_dim_contract(self):
        # Execute the procedure
        self.cursor.execute("CALL testing.update_dim_contract();")
        self.conn.commit()

        # Verify the results
        self.cursor.execute("SELECT contract_id, date_created, status, updated_date FROM testing.dim_contract ORDER BY contract_id;")
        results = self.cursor.fetchall()

        expected_results = [
            (1, date(2024, 6, 15), 'active', date(2024, 6, 15)),
            (2, date(2024, 6, 15), 'active', date(2024, 6, 15))
        ]

        self.assertEqual(results, expected_results)

if __name__ == '__main__':
    unittest.main()