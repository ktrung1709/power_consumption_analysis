from confluent_kafka import Consumer, KafkaError
import os

# Kafka broker configuration
bootstrap_servers = 'localhost:29092,localhost:39092'
topic = 'power_consumption_topic'

# Kafka consumer configuration
conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'power_consumption_group',
    'auto.offset.reset': 'earliest'  # Start consuming from the beginning of the topic
}

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
            print(f'Message received: {message.value().decode("utf-8")}')
    except KeyboardInterrupt:
        pass
    finally:
        kafka_consumer.close()

if __name__ == '__main__':
    main()
