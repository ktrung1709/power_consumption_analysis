import psycopg2

# Connect to Amazon Redshift database
db = psycopg2.connect(
    host="redshift-cluster-1.cbkd07elg7lb.ap-southeast-1.redshift.amazonaws.com",
    port="5439",
    user="awsuser",
    password="Ktrung1709",
    dbname="dev"
)

# Define number of contracts and customers
num_customers = 10200

# Define date_created and status
date_created = '2023-01-01'
status = 'active'

# Generate and insert data into contract table
cursor = db.cursor()
for customer_id in range(1, num_customers + 1):
    sql = "INSERT INTO cmis.contract (contract_id, customer_id, date_created, status) VALUES (%s, %s, %s, %s)"
    val = (customer_id, customer_id, date_created, status)
    cursor.execute(sql, val)

db.commit()
print(f"One contract inserted for each of the {num_customers} customers.")

# Close database connection
db.close()

