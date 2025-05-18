# Kafka Weather Streaming (CS 544 Project P7)

In this project, a Kafka-based pipeline is developed to simulate weather station data streams. A Kafka producer emits daily max temperature records, and multiple consumer processes consume the data, compute rolling statistics, and write partition-wise JSON files. The pipeline includes support for checkpointing and crash recovery to achieve exactly-once processing semantics.

---

## 📚 Learning Objectives

- Write Kafka producers and consumers in Python  
- Apply streaming techniques with exactly-once semantics  
- Use manual and automatic Kafka partition assignment  
- Maintain state and perform checkpointing for crash recovery  

---

## ⚙️ Setup Instructions

### 1. Build the Docker Image with Kafka

```bash
docker build . -t p7
```
### 1. Run Kafka Broker with Source Mapping

```bash
docker run -d -v ./src:/src --name=p7 p7
```

## 🧱 Project Structure
- `src/producer.py`: Kafka producer that writes protobuf-encoded weather data
- `src/consumer.py`: Consumer that reads partitions, aggregates stats, and writes JSON
- `src/debug.py`: Debug consumer that prints messages to console 
- `src/report.proto`: Protobuf definition for weather records
- `src/weather.py`: Provided utility for weather generation
- `src/partition-N.json`: Rolling JSON summary per partition
- `.gitignore`: To exclude generated or temp files


## 🛠️ Technologies Used
- Apache Kafka (single-broker mode)
- Python `kafka-python` client
- gRPC Protobuf for binary serialization
- JSON for stat output
- Docker

## 🚦 Execution Instructions

### Producer (runs infinitely in background)
```bash
docker exec -d -w /src p7 python3 producer.py
```
### Debug Consumer (automatic partition assignment)

```bash
docker exec -it -w /src p7 python3 debug.py
```

### Stats Consumer (manual partition assignment)

```bash
# Example: reads partitions 0 and 2
docker exec -it -w /src p7 python3 consumer.py 0 2
```

## 🧩 Key Features

### ✅ Protobuf Encoding

- Data is serialized using `.SerializeToString()` from a custom `Report` protobuf schema.

### ✅ Kafka Producer Settings

- Retries: `10`  
- Acknowledgments: `all` (for in-sync replicas)  
- Message Key: `station_id` (used for partitioning)

### ✅ Consumer Functionality

- **Manual Partition Assignment** using `assign` and CLI args  
- **Checkpointing** using Kafka offset + local JSON  
- **Crash Recovery** using `seek(offset)` if previous state exists  
- **Atomic Writes** using `.tmp` + `os.rename` to prevent partial reads





