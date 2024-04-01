from confluent_kafka import Producer
import json
import time
import random
from datetime import datetime
import mysql.connector

# Kafka broker configuration
bootstrap_servers = 'localhost:29092,localhost:39092'
topic = 'power_consumption_topic'

# Kafka producer configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': 'power_consumption_producer'
}

# MySQL database configuration
mysql_host = 'operational-instance.cb6okkecsgxd.ap-southeast-1.rds.amazonaws.com'
mysql_user = 'admin'
mysql_password = 'Ktrung1709'
mysql_database = 'cmis'

def delivery_report(err, msg):
    if err is not None:
        print(f'Failed to deliver message: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def fetch_meter_ids():
    # Connect to MySQL database
    try:
        connection = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
        cursor = connection.cursor()

        # Fetch meter IDs from the electric meter table
        cursor.execute('SELECT meter_id FROM electric_meter')
        meter_ids = [row[0] for row in cursor.fetchall()]

        return meter_ids
    except mysql.connector.Error as error:
        print(f'Error fetching meter IDs: {error}')
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def generate_power_consumption_data(meter_id):
    # Generate simulated power consumption data
    date_time = datetime.now()
    date = date_time.strftime('%d/%m/%Y')
    time_now = date_time.strftime('%H:%M:%S')
    global_active_power = round(random.uniform(0.0, 10.0), 2)  # Kilowatt
    global_reactive_power = round(random.uniform(0.0, 5.0), 2)  # Kilowatt
    voltage = round(random.uniform(200.0, 240.0), 2)  # Volt
    global_intensity = round(random.uniform(0.0, 50.0), 2)  # Ampere
    sub_metering_1 = round(random.uniform(0.0, 1000.0), 2)  # Watt-hour
    sub_metering_2 = round(random.uniform(0.0, 1000.0), 2)  # Watt-hour
    sub_metering_3 = round(random.uniform(0.0, 1000.0), 2)  # Watt-hour
    
    return {
        'meter_id': meter_id,
        'date': date,
        'time': time_now,
        'global_active_power': global_active_power,
        'global_reactive_power': global_reactive_power,
        'voltage': voltage,
        'global_intensity': global_intensity,
        'sub_metering_1': sub_metering_1,
        'sub_metering_2': sub_metering_2,
        'sub_metering_3': sub_metering_3
    }

def main():
    kafka_producer = Producer(conf)
    try:
        while True:
            # Fetch meter IDs from the electric meter table
            meter_ids = fetch_meter_ids()
            if meter_ids:
                # Generate power consumption data for each meter
                for meter_id in meter_ids:
                    power_consumption_data = generate_power_consumption_data(meter_id)
                    # Convert data to JSON
                    message = json.dumps(power_consumption_data)
                    # Produce message to Kafka topic
                    kafka_producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
            
            # Wait for 1 hour before producing the next set of messages
            time.sleep(3600)  # 3600 seconds = 1 hour
    except KeyboardInterrupt:
        pass
    finally:
        kafka_producer.flush()
        kafka_producer.close()

if __name__ == '__main__':
    main()
