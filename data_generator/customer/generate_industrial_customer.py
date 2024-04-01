import mysql.connector
from faker import Faker

# Connect to MySQL database
db = mysql.connector.connect(
    host="operational-instance.cb6okkecsgxd.ap-southeast-1.rds.amazonaws.com",
    user="admin",
    password="Ktrung1709",
    database="cmis"
)

# Create Faker instance
fake = Faker()

# Define customer type
customer_type = 'industrial'

# Define number of records to generate
num_records = 100

# Generate and insert data into customer table
cursor = db.cursor()
for i in range(10101, num_records + 10101):
    # Generate industrial-sounding name
    company_name = fake.company()
    industry_term = fake.random_element(elements=("Manufacturing", "Industrial", "Engineering", "Tech", "Machinery", "Construction", "Fabrication", "Steel", "Metalworks", "Processing", "Chemical", "Utilities", "Power", "Plant", "Supply", "Logistics"))
    customer_name = f"{company_name} {industry_term}"
    
    phone = fake.phone_number()
    email = fake.email()
    address = fake.address()
    
    sql = "INSERT INTO customer (customer_id, customer_name, phone, email, address, customer_type) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (i, customer_name, phone, email, address, customer_type)
    cursor.execute(sql, val)
    
    # Print progress
    if i % 100 == 0:
        print(f"{i} records inserted.")

db.commit()
print(f"{num_records} records inserted into the customer table.")

# Close database connection
db.close()