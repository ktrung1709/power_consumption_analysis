from confluent_kafka import Producer
import json
import csv
import random
import time
from datetime import datetime, timedelta

# Load meter IDs from CSV file
meter_ids = []
with open('data/electric_meter_202404152150.csv', newline='', mode='r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for line in reader:
        if int(line['contract_id']) in range(1, 10001):
            meter_ids.append(line['meter_id'])

# Function to generate random power consumption data for a specific meter ID
def generate_power_consumption(meter_id, hour):
    if hour in range(17,24):
        consumption_factor = 1
    else:
        consumption_factor = 0.6
    measure = random.uniform(2.0, 5.0) * consumption_factor
    datetime_measured = datetime(2023, 1, 1, hour, 0, 0).strftime('%Y-%m-%d %H:%M:%S')
    return {
        'meter_id': meter_id,
        'measure': measure,
        'datetime_measured': datetime_measured
    }

# Kafka broker configuration
bootstrap_servers = 'localhost:29092,localhost:39092'
topic = 'power_consumption_topic'

# Kafka producer configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': 'power_consumption_producer'
}

def main():
    kafka_producer = Producer(conf)
    try:
        while True:
            for meter_id in meter_ids:
                for hour in range(0,24):
                    power_consumption_data = generate_power_consumption(meter_id, hour)
                    # Convert data to JSON
                    message = json.dumps(power_consumption_data)
                    # Produce message to Kafka topic
                    kafka_producer.produce(topic, message.encode('utf-8'))
                    kafka_producer.poll(0)
            break
    except KeyboardInterrupt:
        pass
    finally:
        kafka_producer.flush()
        kafka_producer.poll(timeout=1)


if __name__ == '__main__':
    main()