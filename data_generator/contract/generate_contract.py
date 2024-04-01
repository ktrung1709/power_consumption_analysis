import mysql.connector

# Connect to MySQL database
db = mysql.connector.connect(
    host="operational-instance.cb6okkecsgxd.ap-southeast-1.rds.amazonaws.com",
    user="admin",
    password="Ktrung1709",
    database="cmis"
)

# Define number of contracts and customers
num_customers = 10200

# Define date_created and status
date_created = '2023-01-01'
status = 'active'

# Generate and insert data into contract table
cursor = db.cursor()
for customer_id in range(1, num_customers + 1):
    sql = "INSERT INTO contract (contract_id, customer_id, date_created, status) VALUES (%s, %s, %s, %s)"
    val = (customer_id, customer_id, date_created, status)
    cursor.execute(sql, val)

db.commit()
print(f"One contract inserted for each of the {num_customers} customers.")

# Close database connection
db.close()

