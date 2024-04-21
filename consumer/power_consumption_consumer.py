from confluent_kafka import Consumer, KafkaError
from datetime import datetime, timedelta
import boto3
import json
# Kafka broker configuration
bootstrap_servers = 'localhost:29092,localhost:39092'
topic = 'power_consumption_topic1'

# Kafka consumer configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'power_consumption_group',
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
}

access_key_id = 'AKIATMFNNGPO53WMF6WR'
secret_access_key = '1UP/8BR0A3zy11lqjT7jcMWR8IhZR+NR+h/NBcPA'
region = 'ap-southeast-1'  # Example: 'us-east-1'
bucket_name = 'electricity-consumption-master-data'

# AWS S3 configuration
s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

# Variables to store messages received within a time window
hourly_messages = []

def send_to_s3(data):
    file_name = f'power_consumption_data_{datetime.now().strftime("%Y%m%d%H%M%S")}.json'
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=data)

def process_hourly_data():
    if hourly_messages:
        # Convert hourly_messages to JSON
        json_data = json.dumps(hourly_messages)
        # Send JSON data to S3
        send_to_s3(json_data)
        # Clear hourly_messages for the next time window
        hourly_messages.clear()

def main():
    kafka_consumer = Consumer(conf)
    kafka_consumer.subscribe([topic])

    try:
        while True:
            message = kafka_consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    continue
                else:
                    print(f'Error: {message.error()}')
                    break
            # Add message value (power consumption data) to hourly_messages list
            hourly_messages.append(json.loads(message.value()))

            # Check if an hour has passed since the first message received
            if hourly_messages and datetime.now() - datetime.strptime(hourly_messages[0]['datetime_measured'], '%Y-%m-%d %H:%M:%S') >= timedelta(minutes=15):
                # Process the hourly data and send it to S3
                process_hourly_data()
                print("Data uploaded to S3")
    except KeyboardInterrupt:
        pass
    finally:
        # Process any remaining hourly data
        process_hourly_data()
        kafka_consumer.close()

if __name__ == '__main__':
    main()

