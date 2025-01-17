# Spark Demo

This repository contains example projects demonstrating various Apache Spark features using Python (`PySpark`). These examples cover Spark SQL, RDD, DataFrame, and Structured Streaming with Kafka integration.

## Prerequisites
- Python 3.8+
- Apache Spark 3.5.4
- Java 8 or higher
- Kafka (for streaming example)

### Install Python dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install pyspark kafka-python
```

### Start Kafka and Zookeeper
$ brew services start zookeeper
$ brew services start kafka

## Usage

1. DataFrame Example

Basic DataFrame creation and transformation.

$ python dataframe.py

2. SQL Example

Run SQL operations on Spark DataFrames.

$ python sql.py

3. Streaming Example

Stream data from Kafka.

Start Kafka Producer:

$ kafka-console-producer --broker-list localhost:9092 --topic student_topic

Run Spark Streaming:

$ spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 streaming.py

4. RDD Example

Run word count using RDD.

$ python rdd.py

## Configuration
Ensure the following environment variables are properly configured:
JAVA_HOME
SPARK_HOME

## Features
Apache Spark SQL
RDD transformations and actions
DataFrame operations
Structured Streaming with Kafka

## License
This project is licensed under the MIT License.


