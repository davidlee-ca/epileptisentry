from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from json import loads
import numpy as np
import pywt
import entropy


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
    return delta_ApEns


def analyze_sample(rdd):

    if not(rdd.isEmpty()):

        # Schema for the dataframe
        schema = StructType([
                StructField("patient_id", StringType(), nullable=False),
                StructField("channel", StringType(), nullable=False),
                StructField("timestamp", FloatType(), nullable=False),  # will turn into DateTime eventually
                StructField("voltage", FloatType(), nullable=True)
        ])

        df_input = spark.createDataFrame(rdd, schema)
        df_input.show()

        readings = [(row.timestamp, row.voltage) for row in df_input.collect()]  # extract the readings
        readings_sorted = sorted(readings, key=lambda v: v[0])  # sort by timestamp: it should be mostly sorted already
        timeseries = [v[1] for v in readings_sorted]  # extract only the time series
        seizure_indicator = get_delta_apen(timeseries)  # calculate the delta-approx. entropy as the seizure indicator

        # Print out the results
        print(f"Readings: {readings}")
        print(f"  sorted: {readings_sorted}")
        print(f"Time Series: {timeseries}")
        print(f"There are {len(timeseries)} elements in this series.")
        print(f"Seizure indicator = {seizure_indicator}")


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
