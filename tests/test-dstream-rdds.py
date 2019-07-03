from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from json import loads
from datetime import datetime as dt

"""
from pyspark.sql.types import *
import operator
import numpy as np
import pywt
import entropy
import os
import sys


postgres_url="jdbc:postgresql://ip-10-0-1-31.ec2.internal:5432/speegs"
postgres_properties = {
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD']
}

def postgres_batch_raw(df, epoch_id):
    # foreachBatch write sink; helper for writing streaming dataFrames
    df.write.jdbc(
        url=postgres_url,
        table="eeg_data",
        mode="append",
        properties=postgres_properties)

def postgres_batch_analyzed(df, epoch_id):
    # foreachBatch write sink; helper for writing streaming dataFrames
    df.write.jdbc(
        url=postgres_url,
        table="eeg_analysis",
        mode="append",
        properties=postgres_properties)
"""


kafka_brokers="ip-10-0-1-24.ec2.internal:9092,ip-10-0-1-62.ec2.internal:9092,ip-10-0-1-17.ec2.internal:9092"


def analyze_sample(rdd):
    if rdd.isEmpty():
        print("Received an empty RDD")
    else:
        rdd.pprint(30)


if __name__ == "__main__":

    subject_id = "chb01"
    topic = "eeg-signal"

    sc = SparkContext(appName="SparkStreamConsumerFromKafka").getOrCreate()
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 2)
    spark = SparkSession(sc)

    raw_topic = KafkaUtils.createStream(ssc,
                                        kafka_brokers,
                                        "sparkTestApplication", {"eeg-signal": 1})
    parsed_input = raw_topic.map(lambda v: (loads(v[0]), loads(v[1])))  # expect JSON serialization

    # structure: subject_id, channel, instrument_timestamp, voltage
    parsed_input = parsed_input.map(
        lambda v: (v[0]["subject"], v[0]["ch"], dt.fromtimestamp(v[1]["timestamp"]), v[1]["v"]))

    parsed_input.pprint(30)

    # Create sliding window of 16 seconds and run it every 4 seconds
    sliding_window_input = filtered_input.window(16, 4)

    sliding_window_input.pprint(30)

    # Make the call to the analysis function
    sliding_window_input.foreachRDD(analyze_sample)

    ssc.start()
    ssc.awaitTermination()

"""
    # Create a local SparkSession:
    # https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example
    spark = SparkSession \
        .builder \
        .appName("ProcessSingleSubject") \
        .getOrCreate()

    # Suppress the console output's INFO and WARN
    # https://stackoverflow.com/questions/27781187/how-to-stop-info-messages-displaying-on-spark-console
    spark.sparkContext.setLogLevel("WARN")

    # Subscribe to a Kafka topic:
    # https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-streaming-queries
    dfstream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribe", topic) \
        .load()

    # Parse this into a schema: channel from key, instrument timestamp and voltage from value
    # You can parson JSON using DataFrame functions
    # https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.get_json_object
    dfstreamStr = dfstream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    dfParse = dfstreamStr.select(
        get_json_object(dfstreamStr.key, "$.ch").cast(StringType()).alias("channel"),
        from_unixtime(get_json_object(dfstreamStr.value, "$.timestamp").cast(DoubleType())).cast(TimestampType()).alias("instr_time"),
        get_json_object(dfstreamStr.value, "$.v").cast(FloatType()).alias("voltage")
    )

    # Window this into 16-second windows hopping at 4 seconds -- INSTRUMENT TIME, not real time!
    # It will also ensure that I will have 4096 data points at each bin
    #
    # Group by subject and and then by channel
    # You can group by multiple columns:
    # https://stackoverflow.com/questions/41771327/spark-dataframe-groupby-multiple-times
    dfWindow = dfParse \
        .withWatermark("instr_time", "4 seconds") \
        .groupby(window(col("instr_time"), "16 seconds", "4 seconds"), "channel") \
        .agg(collect_list(struct("instr_time", "voltage")).alias("time_series"))

    # Help function that will sort the grouped time series
    # https://stackoverflow.com/questions/46580253/collect-list-by-preserving-order-based-on-another-variable
    def sort_and_analyze_time_series(l):
        if len(l) < 3687: # ideally this needs to be 4096; 10% tolerance
            return None
        res = sorted(l, key=operator.itemgetter(0))
        time_series = [item[1] for item in res]
        seizure_indicator = get_delta_apen(time_series)
        return seizure_indicator

    analyze_udf = udf(sort_and_analyze_time_series)
    count_udf = udf(lambda x: len(x))

    # Apply the UDF to the time series of voltage and obtain the seizure metric
    dfAnalysis = dfWindow.select(
        col("window.end").alias("instr_time"),
        "subject_id",
        "channel",
        analyze_udf(dfWindow.time_series).cast(FloatType()).alias("seizure_metric"),
        count_udf(dfWindow.time_series).cast(IntegerType()).alias("num_datapoints")
    )

    # For appending to the analysis results, filter out null (not ready to commit).
    dfAnalysisFiltered = dfAnalysis.where("seizure_metric is not null")

    # Pass on raw data in periodic batches
    dfRawWrite = dfParse.writeStream \
        .outputMode("append") \
        .foreachBatch(postgres_batch_raw) \
        .trigger(processingTime="5 seconds") \
        .start()

    # for dfAnalysis, write as soon as they become available
    dfAnalysisWrite = dfAnalysis.writeStream \
        .outputMode("append") \
        .foreachBatch(postgres_batch_analyzed) \
        .trigger(processingTime="5 seconds") \
        .start()

    dfRawWrite.awaitTermination()
    dfAnalysisWrite.awaitTermination()
"""