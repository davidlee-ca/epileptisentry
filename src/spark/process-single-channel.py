from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from json import loads
from pyspark.sql import dataframe as d


def analyze_sample(rdd):
    if rdd.isEmpty():
        print("RDD is empty")
    else:
        df = rdd.toDF()
        df.show()


if __name__ == "__main__":

    sc = SparkContext(appName="SparkStreamConsumerFromKafka").getOrCreate()
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 2)

    raw_topic = KafkaUtils.createStream(ssc, "10.0.0.13:2181", "sparkApplication", {"eeg-signal": 1})
    parsed_input = raw_topic.map(lambda v: (loads(v[0]), loads(v[1]))) # expect JSON serialization

    # structure: subject_id, channel, instrument_timestamp, voltage
    parsed_input = parsed_input.map(lambda v: (v[0]["subject"], v[0]["ch"], v[1]["timestamp"], v[1]["v"]))
    parsed_input.pprint()

    # for MVP - filter a single
    filtered_input = parsed_input.filter(lambda x: (x[0] == "chb01") and (x[1] == "FP1-F3"))
    filtered_input.pprint()

    # Create sliding window of 16 seconds, every 4 seconds
    sliding_window_input = filtered_input.window(16, 4)
    sliding_window_input.pprint()

    # Convert to dataframe and start analyzing
    # df_input = sliding_window_input.toDF(["subject_id", "channel", "timestamp", "voltage"]).collect()

    analyze_sample(sliding_window_input)


    ssc.start()
    ssc.awaitTermination()
