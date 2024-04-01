from confluent_kafka import Producer
import json
import time
import random

# Kafka broker configuration
bootstrap_servers = 'localhost:29092,localhost:39092'
topic = 'power_consumption_topic'

# Kafka producer configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': 'power_consumption_producer'
}

def delivery_report(err, msg):
    if err is not None:
        print(f'Failed to deliver message: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def generate_power_consumption_data():
    # Generate simulated power consumption data
    customer_id = random.randint(1, 100)
    meter_id = random.randint(1, 1000)
    measure_before = random.uniform(0.0, 1000.0)
    measure_after = random.uniform(measure_before, 2000.0)
    datetime_measured = int(time.time())
    return {
        'customer_id': customer_id,
        'meter_id': meter_id,
        'measure_before': measure_before,
        'measure_after': measure_after,
        'datetime_measured': datetime_measured
    }

def main():
    kafka_producer = Producer(conf)
    try:
        while True:
            # Generate power consumption data
            power_consumption_data = generate_power_consumption_data()
            # Convert data to JSON
            message = json.dumps(power_consumption_data)
            # Produce message to Kafka topic
            kafka_producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
            # Wait for a short period before producing the next message
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        kafka_producer.flush()
        kafka_producer.close()

if __name__ == '__main__':
    main()
