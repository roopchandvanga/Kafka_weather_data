from kafka import KafkaConsumer
from kafka import TopicPartition
from report_pb2 import *
import json
import os
import sys

broker = "localhost:9092"  # Kafka broker address
consumer = KafkaConsumer(bootstrap_servers=[broker])
num_partitions = sys.argv[1:]

consumer.assign([TopicPartition("temperatures", int(p)) for p in num_partitions])

json_report_file = {"offset": 0}
for part in num_partitions:
    json_object = json.dumps(json_report_file, indent=4)

    # Create empty JSON files for each partition if they do not exist
    if not os.path.exists(f'/src/partition-{part}.json'):
        with open(f"/src/partition-{part}.json", "w") as outfile:
            outfile.write(json_object)
    else:
        for topic_partition in consumer.assignment():
            with open(f'/src/partition-{topic_partition[1]}.json') as json_file:
                json_report_file = json.load(json_file)
            # Seek the consumer to the stored offset
            consumer.seek(topic_partition, json_report_file["offset"])

while True:
    batch = consumer.poll(1000)  # Poll messages from the assigned partitions
    for topic_partition, messages in batch.items():
        for msg in messages:
            message = Report.FromString(msg.value)

            part = topic_partition[1]

            with open(f'/src/partition-{part}.json') as json_file:  # Read the current state of the partition's JSON file
                json_report_file = json.load(json_file)

            json_report_file["offset"] = consumer.position(topic_partition)

            if not message.station_id in json_report_file:
                json_report_file[message.station_id] = {"count": 0, "sum": 0, "start": message.date}
            json_report_file[message.station_id]["count"] += 1
            json_report_file[message.station_id]["sum"] += message.degrees
            json_report_file[message.station_id]["avg"] = json_report_file[message.station_id]["sum"] / json_report_file[message.station_id]["count"]
            json_report_file[message.station_id]["end"] = message.date
            print(json_report_file)  # Printing the updated JSON structure for debugging purposes

            json_object = json.dumps(json_report_file, indent=4)

            # Write the updated data back to the temporary JSON file and replace the original
            with open(f"/src/partition-{part}.json.tmp", "w") as outfile:
                outfile.write(json_object)
                os.rename(f"/src/partition-{part}.json.tmp", f"/src/partition-{part}.json")
