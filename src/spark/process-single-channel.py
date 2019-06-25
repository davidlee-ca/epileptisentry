from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from json import loads


if __name__ == "__main__":

    sc = SparkContext(appName="SparkStreamConsumerFromKafka").getOrCreate()
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 2)

    raw_topic = KafkaUtils.createStream(ssc, "10.0.0.13:2181", "sparkApplication", {"eeg-signal": 1})
    parsed_input = raw_topic.map(lambda v: (loads(v[0]), loads(v[1]))) # expect JSON serialization

    # structure: subject_id, channel, instrument_timestamp, voltage
    parsed_input = parsed_input.map(lambda v: (v[0]["subject"], v[0]["ch"], v[1]["timestamp"], v[1]["v"]))
    parsed_input.count()
    parsed_input.pprint()

    # for MVP - filter a single
    filtered_input = parsed_input.filter(lambda x: (x[0] == "chb01") and (x[1] == "FP1-F3"))
    filtered_input.count()
    filtered_input.pprint()

    # apparently it's easy to do windowing... let's see
    filtered_windowed_input = filtered_input.window(8, 2)
    filtered_windowed_input.pprint()


    ssc.start()
    ssc.awaitTermination()
