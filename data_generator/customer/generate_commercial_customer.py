import psycopg2
from faker import Faker

# Connect to Amazon Redshift database
db = psycopg2.connect(
    host="redshift-cluster-1.cbkd07elg7lb.ap-southeast-1.redshift.amazonaws.com",
    port="5439",
    user="awsuser",
    password="Ktrung1709",
    dbname="dev"
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