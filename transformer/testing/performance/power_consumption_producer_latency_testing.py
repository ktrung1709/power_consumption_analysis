from confluent_kafka import Producer
import csv
import random
import time
from datetime import datetime

# Load meter IDs from CSV file
meter_ids = {}
with open('data/electric_meter_202404152150.csv', newline='', mode='r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for line in reader:
        if int(line['contract_id']) in range(1, 10001):
            meter_ids[line['meter_id']] = "residential"
        elif int(line['contract_id']) in range(10001, 10101):
            meter_ids[line['meter_id']] = "commercial"
        else:
            meter_ids[line['meter_id']] = "industrial"

# Function to generate random power consumption data for a specific meter ID
def generate_power_consumption_csv(meter_id, customer_type):
    dateobj = datetime.now()

    if customer_type == "residential":
        if (dateobj.month-1)//3 + 1 == 1 or (dateobj.month-1)//3 + 1 == 3:
            if dateobj.weekday() in range(0, 5):
                if dateobj.hour in range(17,24):
                    consumption_factor = 1
                else:
                    consumption_factor = 0.6
            else:
                if dateobj.hour in range(8,24):
                    consumption_factor = 1
                else:
                    consumption_factor = 0.6
        else:
            if dateobj.weekday() in range(0, 5):
                if dateobj.hour in range(17,24):
                    consumption_factor = 1.3
                else:
                    consumption_factor = 0.9
            else:
                if dateobj.hour in range(8,24):
                    consumption_factor = 1
                else:
                    consumption_factor = 0.6
        measure = random.uniform(2.0, 5.0) * consumption_factor

    elif customer_type == "commercial":
        if dateobj.weekday() in range(0, 5):
            if dateobj.hour in range(8,18):
                consumption_factor = 1
            else:
                consumption_factor = 0.6
        else:
            consumption_factor = 0.6
        measure = random.uniform(10.0, 100.0) * consumption_factor

    else:
        if dateobj.weekday() in range(0, 5):
            if dateobj.hour in range(8,18):
                consumption_factor = 1
            else:
                consumption_factor = 0.6
        else:
            consumption_factor = 0.6
        measure = random.uniform(50.0, 500.0) * consumption_factor

    datetime_measured = dateobj.strftime('%Y-%m-%d %H:%M:%S')

    return f"{meter_id},{measure},{datetime_measured}"

# Kafka broker configuration
bootstrap_servers = 'localhost:29092,localhost:39092'
topic = 'latency_testing'

# Kafka producer configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': 'power_consumption_producer'
}

def main():
    kafka_producer = Producer(conf)
    try:
        while True:
            # Generate Data for each Meter
            for meter_id in meter_ids.keys():
                power_consumption_data = generate_power_consumption_csv(meter_id, meter_ids[meter_id])
                # Produce message to Kafka topic
                kafka_producer.produce(topic, power_consumption_data.encode('utf-8'))
                print(f"Message sent at {datetime.now()}")
                kafka_producer.poll(0)
            
            # Sleep for 1 hour
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        kafka_producer.flush()
        kafka_producer.poll(timeout=1)


if __name__ == '__main__':
    main()