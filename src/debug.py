
from kafka import TopicPartition, KafkaConsumer
from report_pb2 import Report  

broker = "localhost:9092"
consumer = KafkaConsumer(bootstrap_servers=[broker])
consumer.subscribe(["temperatures"])
#print(consumer.assignment())
#print(consumer.partitions_for_topic("temperatures"))
while True:
    batch = consumer.poll(1000)
    #print(consumer.assignment())
 
    for tp, messages in batch.items():
        partition = tp[1]
        for msg in messages: 
            report = Report.FromString(msg.value)
            output = {
                "station_id": report.station_id,
                "date": report.date,
                "degrees": report.degrees,
                "partition": partition
            }
            print(output)
