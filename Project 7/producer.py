import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, UnknownTopicOrPartitionError
from report_pb2 import Report  # Import the generated Protobuf class
import weather

# Kafka broker configuration
broker = 'localhost:9092'

# Create Kafka producer with necessary configurations
producer = KafkaProducer(
    bootstrap_servers=[broker],
    retries=10,  # Retry up to 10 times if sending the message fails
    acks='all',  # Ensure message is acknowledged only when all in-sync replicas have received it
    key_serializer=lambda station_id: station_id.encode('utf-8'),  # Use the station_id as the key
    value_serializer=lambda v: v.SerializeToString()  # Serialize Protobuf message to bytes
)

# Kafka admin client for topic management
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

# Delete the topic if it exists
try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topic 'temperatures' successfully.")
except UnknownTopicOrPartitionError:
    print("Topic 'temperatures' does not exist, nothing to delete.")

# Wait for the deletion to propagate
time.sleep(3)

# Create a new 'temperatures' topic with 4 partitions and 1 replica
new_topic = NewTopic(name="temperatures", num_partitions=4, replication_factor=1)
admin_client.create_topics([new_topic])
print("Created topic 'temperatures'.")

# List topics to confirm
print("Current topics:", admin_client.list_topics())

# Start generating and sending weather reports
for date, degrees, station_id in weather.get_next_weather(delay_sec=0.1):
    # Create a Protobuf message for the report
    report = Report(
        date=date,
        degrees=degrees,
        station_id=station_id
    )

    try:
        # Send the weather report to Kafka with station_id as the key
        producer.send('temperatures', key=station_id, value=report)
        print(f"Sent data: {date}, {degrees}, {station_id}")
        time.sleep(0.1)  # Adjust the loop delay if necessary
    except KafkaError as e:
        print(f"Error sending message: {e}")

# Close the producer when done (though the loop will run indefinitely)
producer.flush()
producer.close()

