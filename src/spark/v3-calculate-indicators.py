"""
This PySpark logic consumes electroencephalography (EEG) data from Kafka, and compute 
an "abnormality activity indicator" for every subject and channel on a sliding time window.

The abnormality activity indicator logic has been adapted from Ocak, Exp System Appl 2009.

Author:
David Lee, Insight Data Engineering Fellow, New York City 2019
"""


from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import operator
import numpy as np
import pywt
import entropy
import os


# from StackExchange -- generate surrogate series
# https://stats.stackexchange.com/questions/204032/surrogate-time-series-using-fourier-transform
def generate_surrogate_series(ts):  # ts is a time series signal, represented by an ordered list
    ts_fourier  = np.fft.rfft(ts)
    random_phases = np.exp(np.random.uniform(0, np.pi, len(ts) // 2 + 1) * 1.0j)
    ts_fourier_new = ts_fourier * random_phases
    new_ts = np.fft.irfft(ts_fourier_new)
    return new_ts.tolist()


# Perform discrete wavelet transform to get D2 coefficient time series, and create a matching surrogate time series.
# The difference in the approximate entropy (delta_apen) of the time series pair can identify abnormal brain activity.
def get_delta_ap_en(ts):
    d2_coefficients = pywt.downcoef('d', ts, 'db4', level=2)
    surrogate_coefficients = generate_surrogate_series(d2_coefficients)
    app_entropy_sample = entropy.app_entropy(d2_coefficients, order=2, metric='chebyshev')
    app_entropy_surrogate = entropy.app_entropy(surrogate_coefficients, order=2, metric='chebyshev')

    # Return the delta
    delta_ap_en = app_entropy_surrogate - app_entropy_sample
    return delta_ap_en.item()


# foreachBatch write sink; helper function for writing streaming dataFrames
def postgres_batch_analyzed(df, epoch_id):
    df.write.jdbc(
        url="jdbc:postgresql://10.0.1.38:5432/epileptisentry",
        table="eeg_analysis",
        mode="append",
        properties={
            "user": os.environ['POSTGRES_USER'],
            "password": os.environ['POSTGRES_PASSWORD']
        })


if __name__ == "__main__":
    # Create a local SparkSession
    # https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example
    spark = SparkSession \
        .builder \
        .appName("epileptiSentryProcessV3") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Subscribe to a Kafka topic
    dfstream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                "10.0.1.36:9092,10.0.1.11:9092,10.0.1.27:9092,10.0.1.61:9092,10.0.1.54:9092") \
        .option("subscribe", "eeg-signal") \
        .option("includeTimestamp", "true") \
        .load()

    # Parse this into a schema using Spark's JSON decoder:
    #   - from key -- subject ID and EEG channel
    #   - from value -- instrument timestamp and voltage reading
    #   - the ingestion timestamp
    dfstream_str = dfstream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp AS ingest_time")
    df_parsed = dfstream_str.select(
        dfstream_str.ingest_time,
        get_json_object(dfstream_str.key, "$.subject").cast(StringType()).alias("subject_id"),
        get_json_object(dfstream_str.key, "$.ch").cast(StringType()).alias("channel"),
        from_unixtime(get_json_object(dfstream_str.value, "$.timestamp").cast(DoubleType())).cast(TimestampType()).alias("instr_time"),
        get_json_object(dfstream_str.value, "$.v").cast(FloatType()).alias("voltage")
    )

    # Create 16-second windows hopping at 4 seconds at ingestion time rather than instrument time to avoid situations
    # where EEG signals from one subject miss the watermark time altogether and then never catch up
    # Within each time window, group by subject and and then by channel
    # https://stackoverflow.com/questions/41771327/spark-dataframe-groupby-multiple-times
    df_windowed = df_parsed \
        .withWatermark("ingest_time", "4 seconds") \
        .groupby(window(col("ingest_time"), "16 seconds", "4 seconds"), "subject_id", "channel") \
        .agg(max("instr_time").alias("instr_time"),
             max("ingest_time").alias("ingest_time"),
             count("voltage").alias("num_datapoints"),
             collect_list(struct("instr_time", "voltage")).alias("time_series"))

    # Help function that will sort the grouped time series and compute the seizure indicator metric
    # Sorting is required: due to shuffling, collect_list is not deterministic
    # https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.collect_list
    # https://stackoverflow.com/questions/46580253/collect-list-by-preserving-order-based-on-another-variable

    def sort_and_analyze_time_series(l):
        if len(l) < 3687: # ideally this needs to be 4096; 10% tolerance
            return None
        res = sorted(l, key=operator.itemgetter(0))
        time_series = [item[1] for item in res]
        abnormality_indicator = get_delta_ap_en(time_series)
        return abnormality_indicator

    analyze_udf = udf(sort_and_analyze_time_series)

    # Apply the UDF to the time series of voltage and obtain the seizure metric
    df_analyzed = df_windowed \
        .withColumn("time_series", analyze_udf(col(time_series)).cast(FloatType()).alias("abnormality_indicator"))

    # write the abnormality metrics to TimescaleDB at a 4-second batch
    df_write = df_analyzed \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(postgres_batch_analyzed) \
        .trigger(processingTime="4 seconds") \
        .start()

    df_write.awaitTermination()
