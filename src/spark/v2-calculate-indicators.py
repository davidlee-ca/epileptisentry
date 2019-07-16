from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime as dt
from json import loads
import operator
import numpy as np
import pywt
import entropy
import os


# from StackExchange: generate surrogate series
def generate_surrogate_series(ts):
    ts_fourier = np.fft.rfft(ts)
    random_phases = np.exp(np.random.uniform(0, np.pi, len(ts) // 2 + 1) * 1.0j)
    ts_fourier_new = ts_fourier * random_phases
    new_ts = np.fft.irfft(ts_fourier_new)
    return new_ts.tolist()


# Evaluate the approximate entropy of an EEG waveform's D2 coefficients and compare it to its corresponding
# surrogate series, which can indicate the likelihood of seizure activity
# Adapted from Ocak, Exp System Appl 2009
def get_delta_apen(ts): # time-series is an array

    # Perform discrete wavelet transform to get D2 coefficient time series,
    cD2 = pywt.downcoef('d', ts, 'db4', level=2)

    # and create corresponding surrogate time series
    cD2_surrogate = generate_surrogate_series(cD2)
    app_entropy_sample = entropy.app_entropy(cD2, order=2, metric='chebyshev')
    app_entropy_surrogate = entropy.app_entropy(cD2_surrogate, order=2, metric='chebyshev')

    # Return the delta
    delta_ApEns = app_entropy_surrogate - app_entropy_sample
    return delta_ApEns.item()


# Data schema for the DataFrame
schema = StructType([
    StructField("subject_id", StringType(), nullable=False),
    StructField("channel", StringType(), nullable=False),
    StructField("instr_time", TimestampType(), nullable=False),  # will turn into DateTime eventually
    StructField("voltage", FloatType(), nullable=True)
])

# TimescaleDB parameters
tsdb_url="jdbc:postgresql://ip-10-0-1-31.ec2.internal:5432/speegs"

tsdb_properties = {
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD']
}


# Helper function to convert a windowed RDD into DataFrame, compute the seizure indicator, and write to database
def analyze_sample(rdd):

    if not (rdd.isEmpty()):

        df_input = spark.createDataFrame(rdd, schema)

        df_grouped = df_input \
            .groupby("subject_id", "channel") \
            .agg(max("instr_time").alias("instr_time"),
                 count("voltage").alias("num_datapoints"),
                 collect_list(struct("instr_time", "voltage")).alias("time_series"))

        # Help function that will sort the grouped time series
        # https://stackoverflow.com/questions/46580253/collect-list-by-preserving-order-based-on-another-variable
        def sort_and_analyze_time_series(l):
            if len(l) < 3687:  # ideally this needs to be 4096; 10% tolerance
                return None
            res = sorted(l, key=operator.itemgetter(0))
            time_series = [item[1] for item in res]
            seizure_indicator = get_delta_apen(time_series)
            return seizure_indicator

        analyze_udf = udf(sort_and_analyze_time_series)

        # Apply the UDF to the time series of voltage and obtain the seizure metric
        df_analyzed = df_grouped.select(
            "instr_time",
            "subject_id",
            "channel",
            analyze_udf(df_grouped.time_series).cast(FloatType()).alias("seizure_metric"),
            "num_datapoints"
        )

        df_input.write.jdbc(url=tsdb_url, table="eeg_data", mode="append", properties=tsdb_properties)
        df_analyzed.write.jdbc(url=tsdb_url, table="eeg_analysis", mode="append", properties=tsdb_properties)



if __name__ == "__main__":

    topic = "eeg-signal"
    zk_quorum = "10.0.1.62:2181,10.0.1.24:2181,10.0.1.35:2181,10.0.1.17:2181,10.0.1.39:2181"
    sc = SparkContext(appName="SparkStreamConsumerFromKafka").getOrCreate()
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 4)
    spark = SparkSession(sc)

    raw_stream = KafkaUtils.createStream(ssc, zk_quorum, "sparkTestApplication", {"eeg-signal": 1})

    # Unpack the JSON from the messages
    # DStream structure: subject_id, channel, instrument_timestamp, voltage
    parsed_stream = raw_stream \
        .map(lambda u: (loads(u[0]), loads(u[1]))) \
        .map(lambda v: (v[0]["subject"], v[0]["ch"], dt.fromtimestamp(v[1]["timestamp"]), v[1]["v"]))

    # Create sliding window of 16 seconds and run it every 4 seconds
    windowed_stream = parsed_stream.window(16, 4)

    # Hand over the windowed RDD to the analysis function
    windowed_stream.foreachRDD(analyze_sample)

    ssc.start()
    ssc.awaitTermination()
