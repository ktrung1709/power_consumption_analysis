from faker import Faker
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

# Create Faker instance
fake = Faker()

# Define customer type
customer_type = 'commercial'

# Define number of records to generate
num_records = 100

# Generate and insert data into customer table
cursor = db.cursor()
for i in range(10001, 10000 + num_records + 1):
    customer_name = fake.company()
    phone = fake.phone_number()
    email = fake.email()
    address = fake.address()
    
    sql = "INSERT INTO cmis.customer (customer_id, customer_name, phone, email, address, customer_type) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (i, customer_name, phone, email, address, customer_type)
    cursor.execute(sql, val)
    
    # Print progress
    if i % 10 == 0:
        print(f"{i} records inserted.")

db.commit()
print(f"{num_records} records inserted into the customer table.")

# Close database connection
db.close()