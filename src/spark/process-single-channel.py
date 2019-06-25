from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from json import loads
from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *


def analyze_sample(rdd):

    if not(rdd.isEmpty()):

        # Schema for the dataframe
        schema = StructType([
                StructField("patient_id", StringType(), nullable=False),
                StructField("channel", StringType(), nullable=False),
                StructField("timestamp", FloatType(), nullable=False),
                StructField("voltage", FloatType(), nullable=True)
        ])

        df_input = spark.createDataFrame(rdd, schema)
        df_input.show()

        timeseries = [row.voltage for row in df_input.collect()]
        print(timeseries)


if __name__ == "__main__":

    sc = SparkContext(appName="SparkStreamConsumerFromKafka").getOrCreate()
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 2)
    spark = SparkSession(sc)

    raw_topic = KafkaUtils.createStream(ssc, "10.0.0.13:2181", "sparkApplication", {"eeg-signal": 1})
    parsed_input = raw_topic.map(lambda v: (loads(v[0]), loads(v[1]))) # expect JSON serialization

    # structure: subject_id, channel, instrument_timestamp, voltage
    parsed_input = parsed_input.map(lambda v: (v[0]["subject"], v[0]["ch"], v[1]["timestamp"], v[1]["v"]))

    # for MVP - filter a single patient & a single channel
    filtered_input = parsed_input.filter(lambda x: (x[0] == "chb01") and (x[1] == "FP1-F3"))

    # Create sliding window of 16 seconds, every 4 seconds
    sliding_window_input = filtered_input.window(16, 4)

    # Make the call to the analysis function
    sliding_window_input.foreachRDD(analyze_sample)

    ssc.start()
    ssc.awaitTermination()
