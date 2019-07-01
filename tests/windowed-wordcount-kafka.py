from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from json import loads

if __name__ == "__main__":

    sc = SparkContext(appName="SparkStreamConsumerFromKafka").getOrCreate()
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)

    input_from_topic = KafkaUtils.createStream(ssc, "10.0.0.13:2181", "sparkApplication", {"words": 1})
    # input_from_topic = KafkaUtils.createStream(ssc, "10.0.0.13:2181", "sparkApplication", {"eeg-signal": 1})
    # brings each message into (k, v)
    input_from_topic.pprint()

    parsed_input = input_from_topic.map(lambda v: (loads(v[0]), loads(v[1])))
    parsed_input.pprint()

    parsed_input = parsed_input.map(lambda v: (v[0], type(v[0]), v[0]['a']))
    parsed_input.pprint()

    ssc.start()
    ssc.awaitTermination()
