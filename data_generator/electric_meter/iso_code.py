import psycopg2

# Dictionary with provinces
vietnam_provinces = {
    "VN-44": "An Giang",
    "VN-43": "Bà rịa Vũng Tàu",
    "VN-54": "Bắc Giang",
    "VN-53": "Bắc Kạn",
    "VN-55": "Bạc Liêu",
    "VN-56": "Bắc Ninh",
    "VN-50": "Bến Tre",
    "VN-31": "Bình Định",
    "VN-57": "Bình Dương",
    "VN-58": "Bình Phước",
    "VN-40": "Bình Thuận",
    "VN-59": "Cà Mau",
    "VN-CT": "Cần Thơ",
    "VN-04": "Cao Bằng",
    "VN-DN": "Đà Nẵng",
    "VN-33": "Đắk Lắk",
    "VN-72": "Đắk Nông",
    "VN-71": "Điện Biên",
    "VN-39": "Đồng Nai",
    "VN-45": "Đồng Tháp",
    "VN-30": "Gia Lai",
    "VN-03": "Hà Giang",
    "VN-63": "Hà Nam",
    "VN-HN": "Hà Nội",
    "VN-23": "Hà Tĩnh",
    "VN-61": "Hải Dương",
    "VN-HP": "Hải Phòng",
    "VN-73": "Hậu Giang",
    "VN-SG": "TP. Hồ Chí Minh",
    "VN-14": "Hòa Bình",
    "VN-66": "Hưng Yên",
    "VN-34": "Khánh Hòa",
    "VN-47": "Kiến Giang",
    "VN-28": "Kon Tum",
    "VN-01": "Lai Châu",
    "VN-35": "Lâm Đồng",
    "VN-09": "Lạng Sơn",
    "VN-02": "Lào Cai",
    "VN-41": "Long An",
    "VN-67": "Nam Định",
    "VN-22": "Nghệ An",
    "VN-18": "Ninh Bình",
    "VN-36": "Ninh Thuận",
    "VN-68": "Phú Thọ",
    "VN-32": "Phú Yên",
    "VN-24": "Quảng Bình",
    "VN-27": "Quảng Nam",
    "VN-29": "Quảng Ngãi",
    "VN-13": "Quảng Ninh",
    "VN-25": "Quảng Trị",
    "VN-52": "Sóc Trăng",
    "VN-05": "Sơn La",
    "VN-37": "Tây Ninh",
    "VN-20": "Thái Bình",
    "VN-69": "Thái Nguyên",
    "VN-21": "Thanh Hóa",
    "VN-26": "Thừa Thiên Huế",
    "VN-46": "Tiền Giang",
    "VN-51": "Trà Vinh",
    "VN-07": "Tuyên Quang",
    "VN-49": "Vĩnh Long",
    "VN-70": "Vĩnh Phúc",
    "VN-06": "Yên Bái"
}

# Connect to Amazon Redshift database
conn = psycopg2.connect(
    host="redshift-cluster-1.cbkd07elg7lb.ap-southeast-1.redshift.amazonaws.com",
    port="5439",
    user="awsuser",
    password="Ktrung1709",
    dbname="dev"
)

# Create a cursor object
cur = conn.cursor()

# Create table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS vietnam_provinces (
    code VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255)
);
"""
cur.execute(create_table_query)

# Insert data into the table
insert_query = "INSERT INTO vietnam_provinces (code, name) VALUES (%s, %s)"

for code, name in vietnam_provinces.items():
    cur.execute(insert_query, (code, name))

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
