from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from json import loads
from datetime import datetime as dt

kafka_brokers="ip-10-0-1-24.ec2.internal:9092,ip-10-0-1-62.ec2.internal:9092,ip-10-0-1-17.ec2.internal:9092"
zk_quorum="ip-10-0-1-24.ec2.internal:2181,ip-10-0-1-62.ec2.internal:2181,ip-10-0-1-17.ec2.internal:2181"


def analyze_sample(rdd):
    if rdd.isEmpty():
        print("Received an empty RDD")
    else:
        print(f"RDD is {type(rdd)} and has {rdd.count()} rows.")


if __name__ == "__main__":

    subject_id = "chb01"
    topic = "eeg-signal"

    sc = SparkContext(appName="SparkStreamConsumerFromKafka").getOrCreate()
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 2)
    spark = SparkSession(sc)

    raw_topic = KafkaUtils.createStream(ssc,
                                        zk_quorum,
                                        "sparkTestApplication", {"eeg-signal": 1})
    parsed_input = raw_topic.map(lambda v: (loads(v[0]), loads(v[1])))  # expect JSON serialization

    # structure: subject_id, channel, instrument_timestamp, voltage
    parsed_input = parsed_input.map(
        lambda v: (v[0]["subject"], v[0]["ch"], dt.fromtimestamp(v[1]["timestamp"]), v[1]["v"]))

    print(f"Parsed input is {type(parsed_input)}")
    parsed_input.pprint(30)

    # Create sliding window of 16 seconds and run it every 4 seconds
    sliding_window_input = parsed_input.window(16, 4)

    print(f"Sliding window input is {type(sliding_window_input)}")
    sliding_window_input.pprint(30)

    # Make the call to the analysis function
    sliding_window_input.foreachRDD(analyze_sample)

    ssc.start()
    ssc.awaitTermination()
