from kafka import KafkaConsumer, TopicPartition
import json
import sys
import os
from datetime import datetime
from collections import defaultdict
import report_pb2


def main():
    if len(sys.argv) < 2:
        print("Must use at least one partition number")
        sys.exit(1)

    partitions = [int(partition) for partition in sys.argv[1:]]

    broker = 'localhost:9092'
    consumer = KafkaConsumer(bootstrap_servers=[broker])
    topics = [TopicPartition("temperatures", partition) for partition in partitions]
    consumer.assign(topics)

    partition_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    
    # Load existing partition data and offsets
    for partition in partitions:
        partition_file = f"/src/partition-{partition}.json"
        if os.path.exists(partition_file):
            with open(partition_file) as f:
                partition_data[partition] = json.load(f)
        else:
            initial_data = {"offset": 0}
            partition_data[partition] = initial_data
            write_to_json(initial_data, partition_file)

    try:
        while True:
            for message in consumer:
                partition = message.partition
                offset = message.offset
                data = partition_data[partition]
                report = report_pb2.Report()
                report.ParseFromString(message.value)

                station_id = report.station_id
                date = report.date
                degrees = report.degrees

                # Initialize station data if not already present
                if station_id not in data:
                    data[station_id] = {
                        "count": 0,
                        "sum": 0.0,
                        "avg": 0.0,
                        "start": date,  
                        "end": date
                    }
                start_date = data[station_id]["start"]
                if start_date is None:
                    start_date = date
                # Update station statistics
                data[station_id]["count"] += 1
                data[station_id]["sum"] += degrees
                data[station_id]["avg"] = data[station_id]["sum"] / data[station_id]["count"]
                data[station_id]["end"] = date

                # Compare dates correctly
                if start_date > date:
                    data[station_id]["start"] = date
                else:
                    data[station_id]["start"] = start_date
                # Update the offset in the partition data
                data["offset"] = offset + 1  # Save next offset to consume

                # Write the updated data back to the partition file atomically
                partition_file = f"/src/partition-{partition}.json"  # Save in the current directory (/src)
                write_to_json(data, partition_file)

    except KeyboardInterrupt:
        print("keyboard interrupt")
        consumer.close()


def write_to_json(data, file):
    temp_file = file + ".tmp"
    with open(temp_file, "w") as f:
        json.dump(data, f, indent=2, default=datetime_serializer)
    os.rename(temp_file, file)


def datetime_serializer(obj):
    if isinstance(obj, datetime):
        return obj.date().isoformat()


if __name__ == "__main__":
    main()

