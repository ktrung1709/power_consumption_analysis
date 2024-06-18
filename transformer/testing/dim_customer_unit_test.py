import psycopg2
import unittest
from datetime import date

# Replace these with your actual Redshift cluster details
REDSHIFT_HOST = 'redshift-cluster-1.cbkd07elg7lb.ap-southeast-1.redshift.amazonaws.com'
REDSHIFT_PORT = '5439'
REDSHIFT_DBNAME = 'dev'
REDSHIFT_USER = 'awsuser'
REDSHIFT_PASSWORD = 'Ktrung1709'

class TestUpdateDimCustomer(unittest.TestCase):
    
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
            INSERT INTO testing.customer (customer_id, customer_name, phone, email, address, customer_type, updated_date) VALUES
            (1, 'John Doe 1', '123-456-7890', 'john@example.com', '123 Main St', 'Residential', '2024-06-15'),
            (2, 'Jane Smith', '987-654-3210', 'jane@example.com', '456 Elm St', 'Residential', '2024-06-16');

            INSERT INTO testing.dim_customer (customer_id, customer_name, phone, email, address, customer_type, updated_date) VALUES
            (1, 'John Doe', '123-456-7890', 'john@example.com', '123 Main St', 'Residential', '2024-06-14');
        """)
        cls.conn.commit()

    @classmethod
    def tearDownClass(cls):
        # Clean up
        cls.cursor.execute("""
            TRUNCATE TABLE testing.customer;
            TRUNCATE TABLE testing.dim_customer;
        """)
        cls.conn.commit()
        cls.cursor.close()
        cls.conn.close()

    def test_update_dim_customer(self):
        # Execute the procedure
        self.cursor.execute("CALL testing.update_dim_customer();")
        self.conn.commit()

        # Verify the results
        self.cursor.execute("SELECT customer_id, customer_name, phone, email, address, customer_type, updated_date FROM testing.dim_customer ORDER BY customer_id;")
        results = self.cursor.fetchall()

        expected_results = [
            (1, 'John Doe 1', '123-456-7890', 'john@example.com', '123 Main St', 'Residential', date(2024, 6, 15)),
            (2, 'Jane Smith', '987-654-3210', 'jane@example.com', '456 Elm St', 'Residential', date(2024, 6, 16))
        ]

        self.assertEqual(results, expected_results)

if __name__ == '__main__':
    unittest.main()