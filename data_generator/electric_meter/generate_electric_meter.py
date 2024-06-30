import random
import psycopg2
from dotenv import load_dotenv
import os

province_coordinates = {
    "Hà Nội" : (21.0278, 105.8342), 
    "TP. Hồ Chí Minh": (10.8231, 106.6297), 
    "Đà Nẵng": (16.0544, 108.2022), 
    "An Giang": (10.5216, 105.1259),
    "Bà rịa Vũng Tàu": (10.5417, 107.2430),
    "Bắc Giang": (21.2820, 106.1975), 
    "Bắc Kạn": (22.1443, 105.8345), 
    "Bạc Liêu": (9.2940, 105.7216), 
    "Bắc Ninh": (21.1782, 106.0710), 
    "Bến Tre": (10.2434, 106.3756),
    "Bình Định": (13.8860, 109.1077), 
    "Bình Dương": (11.3254, 106.4770), 
    "Bình Phước": (11.7512, 106.7235),
    "Bình Thuận": (11.0904, 108.0721), 
    "Cà Mau": (9.1527, 105.1961), 
    "Cần Thơ": (10.0452, 105.7469), 
    "Cao Bằng": (22.6666, 106.2640), 
    "Đắk Lắk": (12.7100, 108.2378),
    "Đắk Nông": (12.2646, 107.6098), 
    "Điện Biên": (21.4064, 103.0322), 
    "Đồng Nai": (11.0686, 107.1676), 
    "Đồng Tháp": (10.4938, 105.6882), 
    "Gia Lai": (13.8079, 108.1094), 
    "Hà Giang": (22.8026, 104.9784), 
    "Hà Nam": (20.5835, 105.9230), 
    "Hà Tĩnh": (18.3420, 105.8923), 
    "Hải Dương": (20.9373, 106.3146),
    "Hải Phòng": (20.8449, 106.6881), 
    "Hậu Giang": (9.7579, 105.6413), 
    "Hòa Bình": (20.8275, 105.3391), 
    "Hưng Yên": (20.6547, 106.0578), 
    "Khánh Hòa": (12.2585, 109.0526), 
    "Kiên Giang": (9.8250, 105.1259), 
    "Kon Tum": (14.3497, 108.0005), 
    "Lai Châu": (22.3862, 103.4703), 
    "Lâm Đồng": (11.5753, 108.1429),
    "Lạng Sơn": (21.8537, 106.7615), 
    "Lào Cai": (22.4809, 103.9755), 
    "Long An": (10.6956, 106.2431), 
    "Nam Định": (20.4388, 106.1621), 
    "Nghệ An": (19.2342, 104.9200), 
    "Ninh Bình": (20.2506, 105.9745), 
    "Ninh Thuận": (11.6739, 108.8630), 
    "Phú Thọ": (21.4220, 105.2297), 
    "Phú Yên": (13.0882, 109.0929),
    "Quảng Bình": (17.6103, 106.3487), 
    "Quảng Nam": (15.5394, 108.0191), 
    "Quảng Ngãi": (15.1214, 108.8044), 
    "Quảng Ninh": (21.0064, 107.2925), 
    "Quảng Trị": (16.7943, 106.9634), 
    "Sóc Trăng": (9.6025, 105.9739), 
    "Sơn La": (21.3269, 103.9144), 
    "Tây Ninh": (11.3352, 106.1099), 
    "Thái Bình": (20.4463, 106.3366),
    "Thái Nguyên": (21.5672, 105.8252), 
    "Thanh Hóa": (19.8067, 105.7852), 
    "Thừa Thiên Huế": (16.4674, 107.5905), 
    "Tiền Giang": (10.4493, 106.3421), 
    "Trà Vinh": (9.9513, 106.3346), 
    "Tuyên Quang": (21.7767, 105.2280), 
    "Vĩnh Long": (10.2396, 105.9572), 
    "Vĩnh Phúc": (21.3609, 105.5474),
    "Yên Bái": (21.7168, 104.8986)
}

# List of Vietnamese provinces
provinces = list(province_coordinates.keys())

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
# Create cursor
cursor = db.cursor()

# Define SQL statement for inserting data into electric meter table
insert_meter_query = "INSERT INTO cmis.electric_meter (meter_id, contract_id, meter_type, voltage, status, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

# Generate and insert data into the electric meter table
for contract_id in range(1, 10202):
    # Determine meter type based on contract type
    meter_type = "single-phase" if contract_id <= 10000 else "three-phase"
    
    # Determine voltage based on contract type
    if contract_id <= 10000:
        voltage = None  # Residential customers have null voltage
    elif contract_id <= 10100:
        voltage_levels = ["less than 6kV", "6kV to less than 22kV", "22kV and above"]
        voltage = random.choice(voltage_levels)
    else:
        voltage_levels = ["less than 6kV", "6kV to less than 22kV", "22kV to less than 100kV", "100kV and above"]
        voltage = random.choice(voltage_levels)
    
    # Determine number of meters for commercial and industrial customers
    if contract_id > 10000:
        num_meters = random.randint(1, 5)
    else:
        num_meters = 1
    
    # Generate and insert data for each meter
    for meter_num in range(1, num_meters + 1):
        # Generate unique meter ID
        meter_id = (contract_id - 1) * 5 + meter_num
        
        # Determine random location (province) and its coordinates
        location = random.choice(provinces)
        latitude, longitude = province_coordinates[location]
        
        # Insert data into the electric meter table
        cursor.execute(insert_meter_query, (meter_id, contract_id, meter_type, voltage, "active", location, latitude, longitude))
    print(contract_id)    
# Commit changes and close connection
db.commit()
db.close()

print("Data inserted successfully into the electric meter table.")
