import psycopg2
from dotenv import load_dotenv
import os

# Connect to Amazon Redshift database
load_dotenv()
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_DBNAME = os.getenv('REDSHIFT_DBNAME')

db = psycopg2.connect(
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD,
    dbname=REDSHIFT_DBNAME
)

# Define number of contracts and customers
num_customers = 10200

# Define date_created and status
date_created = '2023-01-02'
status = 'active'

# Generate and insert data into contract table
cursor = db.cursor()
for customer_id in range(1, num_customers + 1):
    sql = "INSERT INTO cmis.contract (contract_id, customer_id, date_created, status) VALUES (%s, %s, %s, %s)"
    val = (customer_id, customer_id, date_created, status)
    cursor.execute(sql, val)
    print(customer_id)

db.commit()
print(f"One contract inserted for each of the {num_customers} customers.")

# Close database connection
db.close()

