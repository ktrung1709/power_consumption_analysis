from confluent_kafka import Consumer, KafkaError
from datetime import datetime, timedelta
import boto3

# Kafka broker configuration
bootstrap_servers = 'localhost:29092,localhost:39092'
topic = 'topic1'

# Kafka consumer configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'power_consumption_group',
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
}

# AWS S3 configuration
region = 'ap-southeast-1'  # Example: 'us-east-1'
bucket_name = 'electricity-consumption-master-data'
access_key_id = 'AKIATMFNNGPO53WMF6WR'
secret_access_key = '1UP/8BR0A3zy11lqjT7jcMWR8IhZR+NR+h/NBcPA'
s3_client = boto3.client('s3', region_name=region, aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)



def send_to_s3(data):
    file_name = f'power_consumption_data_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=data.encode('utf-8'))

def main():
    kafka_consumer = Consumer(conf)
    kafka_consumer.subscribe([topic])
    # Initialize hourly_csv_data to store CSV data
    hourly_csv_data = 'meter_id,measure,datetime_measured\n'
    try:
        while True:
            message = kafka_consumer.poll(timeout=1.0)
            if message is None:
                if len(hourly_csv_data) > len('meter_id,measure,datetime_measured\n'):
                    send_to_s3(hourly_csv_data)
                    print("Data uploaded to S3")
                    hourly_csv_data = 'meter_id,measure,datetime_measured\n'
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    continue
                else:
                    print(f'Error: {message.error()}')
                    break
            # Decode and append CSV data to hourly_csv_data
            hourly_csv_data += message.value().decode('utf-8') + '\n'
            print(message.value())
                
    except KeyboardInterrupt:
        pass
    finally:
        kafka_consumer.close()

if __name__ == '__main__':
    main()