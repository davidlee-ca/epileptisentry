from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

"""
from json import loads
from datetime import datetime as dt
import numpy as np
import pywt
import entropy
import os


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
"""
"""
# Schema for the raw data
# in v2, no subject_id -- it'll stay constant
schema = StructType([
    StructField("channel", StringType(), nullable=False),
    StructField("instr_time", TimestampType(), nullable=False),  # will turn into DateTime eventually
    StructField("voltage", FloatType(), nullable=True)
])

postgres_url = "jdbc:postgresql://ip-10-0-1-32.ec2.internal:5432/speegs"
properties = {
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD']
}

# from StackExchange -- generate surrogate series!
def generate_surrogate_series(ts):  # time-series is an array
    ts_fourier  = np.fft.rfft(ts)
    random_phases = np.exp(np.random.uniform(0,np.pi,len(ts)//2+1)*1.0j)
    ts_fourier_new = ts_fourier*random_phases
    new_ts = np.fft.irfft(ts_fourier_new)
    return new_ts.tolist()


def get_delta_apen(ts): # time-series is an array
    # Perform discrete wavelet transform to get A3, D3, D2, D1 coefficient time series,
    # and create corresponding surrogate time series
    (cA1, cD1) = pywt.dwt(ts, 'db4')
    (cA2, cD2) = pywt.dwt(cA1, 'db4')
    cD2_surrogate = generate_surrogate_series(cD2)
    app_entropy_sample = entropy.app_entropy(cD2, order=2, metric='chebyshev')
    app_entropy_surrogate = entropy.app_entropy(cD2_surrogate, order=2, metric='chebyshev')

    # Return the delta
    delta_ApEns = app_entropy_surrogate - app_entropy_sample
    return delta_ApEns.item()


def analyze_sample(rdd):

    if not(rdd.isEmpty()):

        df_input = spark.createDataFrame(rdd, schema)

        readings = [(row.instr_time, row.voltage) for row in df_input.collect()]  # extract the readings
        readings_sorted = sorted(readings, key=lambda v: v[0])  # sort by timestamp: it should be mostly sorted already
        timeseries = [v[1] for v in readings_sorted]  # extract only the time series

        seizure_indicator = get_delta_apen(timeseries)  # calculate the delta-approx. entropy as the seizure indicator
        max_instr_time = df_input.select(max("instr_time")).collect()[0]["max(instr_time)"]  # it's a still a dataframe, so gotta extract the value
        number_of_datapoints = len(timeseries)

        print(f"                 Latest instrument timestamp: {max_instr_time}")
        print(f"     {type(max_instr_time)}")
        print(f"                        There are {len(timeseries)} elements in this series.")
        print(f"                        Seizure indicator = {seizure_indicator}")

        analysis_row = Row(instr_time=max_instr_time, subject_id="chb01", channel="FP1-F3",
                           seizure_metric=seizure_indicator, num_datapoints=number_of_datapoints)
        df_analysis = spark.createDataFrame([analysis_row])

        df_input.write.jdbc(url=postgres_url, table="eeg_data", mode="append", properties=properties)
        df_analysis.write.jdbc(url=postgres_url, table="eeg_analysis", mode="append", properties=properties)

        # Print out the results
#        print(f"Readings: {readings}")
#        print(f"  sorted: {readings_sorted}")
#        print(f"Time Series: {timeseries}")
"""

if __name__ == "__main__":

    # Create a local SparkSession:
    # https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example
    spark = SparkSession \
        .builder \
        .appName("ProcessSingleSubject") \
        .getOrCreate()

    # Suppress the console output's INFO and WARN
    # https://stackoverflow.com/questions/27781187/how-to-stop-info-messages-displaying-on-spark-console
    spark.sparkContext.setLogLevel("ERROR")

    # Subscribe to a Kafka topic:
    # https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-for-streaming-queries
    dfstream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "ip-10-0-1-24.ec2.internal:9092,ip-10-0-1-62.ec2.internal:9092") \
        .option("subscribe", "eeg-signal") \
        .load()
    # full list of Kafka brokers: "ip-10-0-1-24.ec2.internal:9092,ip-10-0-1-62.ec2.internal:9092,ip-10-0-1-17.ec2.internal:9092,ip-10-0-1-35.ec2.internal:9092,ip-10-0-1-39.ec2.internal:9092"

    # Parse this into a schema: channel from key, instrument timestamp and voltage from value
    # You can parson JSON using DataFrame functions
    # https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.get_json_object
    #
    foo = dfstream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    bar = foo.select(
        get_json_object(foo.key, "$subject").cast(StringType()).alias("subject_id"),
        get_json_object(foo.key, "$.ch").cast(StringType()).alias("channel"),
        from_unixtime(get_json_object(foo.value, "$.timestamp").cast(DoubleType())).cast(TimestampType()).alias("instr_time"),
        get_json_object(foo.value, "$.v").cast(FloatType()).alias("voltage")
    )

    bar.printSchema()

    dfstreamWrite = bar\
        .writeStream\
        .format('console')\
        .start()

    dfstreamWrite.awaitTermination()

"""
    dfstreamWindowed = dfstreamRaw \
        .groupby(window()
    )


    sc = SparkContext(appName="SparkStreamConsumerFromKafka").getOrCreate()
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 2)
    spark = SparkSession(sc)

    raw_topic = KafkaUtils.createStream(ssc, "10.0.1.24:2181,10.0.1.62:2181,10.0.1.35:2181,10.0.1.17:2181,10.0.1.39:2181", "sparkApplication", {"eeg-signal": 1})
    parsed_input = raw_topic.map(lambda v: (loads(v[0]), loads(v[1]))) # expect JSON serialization

    # structure: subject_id, channel, instrument_timestamp, voltage
    parsed_input = parsed_input.map(lambda v: (v[0]["subject"], v[0]["ch"], dt.fromtimestamp(v[1]["timestamp"]), v[1]["v"]))

    # for MVP - filter a single patient & a single channel
    filtered_input = parsed_input.filter(lambda x: (x[0] == "chb01") and (x[1] == "FP1-F3"))

    # Create sliding window of 16 seconds and run it every 4 seconds
    sliding_window_input = filtered_input.window(16, 4)

    # Make the call to the analysis function
    sliding_window_input.foreachRDD(analyze_sample)

    ssc.start()
    ssc.awaitTermination()
"""
