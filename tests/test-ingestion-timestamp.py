from pyspark.sql import SparkSession

"""
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import operator
import numpy as np
import pywt
import entropy
import os
"""


if __name__ == "__main__":

    topic = "eeg-signal"
    zk_quorum = "ip-10-0-1-24.ec2.internal:9020,ip-10-0-1-62.ec2.internal:9020,ip-10-0-1-17.ec2.internal:9020"

    # Create a local SparkSession:
    # https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example
    spark = SparkSession \
        .builder \
        .appName("EEGProcessAllSubjectsV3") \
        .getOrCreate()

    # Suppress the console output's INFO and WARN
    # https://stackoverflow.com/questions/27781187/how-to-stop-info-messages-displaying-on-spark-console
    spark.sparkContext.setLogLevel("WARN")

    # Subscribe to a Kafka topic:
    # https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-streaming-queries
    dfstream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "ip-10-0-1-24.ec2.internal:9092,ip-10-0-1-62.ec2.internal:9092") \
        .option("subscribe", "eeg-signal") \
        .option("includeTimestamp", True) \
        .load()

    # Parse this into a schema: channel from key, instrument timestamp and voltage from value
    # You can parson JSON using DataFrame functions
    # https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.get_json_object

    dfconsole = dfstream \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    dfconsole.awaitTermination()
