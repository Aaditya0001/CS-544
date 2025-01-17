from kafka import KafkaConsumer
import json
from report_pb2 import Report  # Import the Protobuf class
from kafka.errors import KafkaError

# Kafka broker configuration
broker = 'localhost:9092'

# Initialize the KafkaConsumer
consumer = KafkaConsumer(
    'temperatures',  # Subscribe to the 'temperatures' topic
    group_id='debug',  # Consumer group name is 'debug'
    bootstrap_servers=[broker],
    auto_offset_reset='latest',  # Start consuming from the latest message
    enable_auto_commit=True,  # Enable auto commit of offsets
    value_deserializer=lambda m: Report().FromString(m),  # Deserialize Protobuf messages to Report objects
)

# Start consuming messages forever
try:
    for message in consumer:
        # Deserialize the message from Protobuf to a Python Report object
        report = message.value

        # Create a dictionary with the message details
        report_dict = {
            'station_id': report.station_id,
            'date': report.date,
            'degrees': report.degrees,
            'partition': message.partition
        }

        # Print the dictionary for the consumed message
        print(report_dict)

except KafkaError as e:
    print(f"Error consuming messages: {e}")
finally:
    consumer.close()  # Ensure the consumer is closed when done

