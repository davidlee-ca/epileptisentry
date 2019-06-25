from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from json import loads


if __name__ == "__main__":

    sc = SparkContext(appName="SparkStreamConsumerFromKafka").getOrCreate()
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 8)

    raw_topic = KafkaUtils.createStream(ssc, "10.0.0.13:2181", "sparkApplication", {"eeg-signal": 1})
    parsed_input = raw_topic.map(lambda v: (loads(v[0]), loads(v[1]))) # expect JSON serialization

    # structure: subject_id, channel, instrument_timestamp, voltage
    parsed_input = parsed_input.map(lambda v: (v[0]["subject"], v[0]["ch"], v[1]["timestamp"], v[1]["v"]))
    parsed_input.pprint()
    parsed_input.count()

    # for MVP - filter a single
    filtered_input = parsed_input.filter(lambda x: (x[0] == "chb01") and (x[1] == "FP1-F3"))
    filtered_input.pprint()
    filtered_input.count()

    ssc.start()
    ssc.awaitTermination()
