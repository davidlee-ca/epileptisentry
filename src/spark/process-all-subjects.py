from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import operator
import numpy as np
import pywt
import entropy
import os


# from StackExchange -- generate surrogate series!
def generate_surrogate_series(ts):  # time-series is an array
    ts_fourier  = np.fft.rfft(ts)
    random_phases = np.exp(np.random.uniform(0,np.pi,len(ts)//2+1)*1.0j)
    ts_fourier_new = ts_fourier*random_phases
    new_ts = np.fft.irfft(ts_fourier_new)
    return new_ts.tolist()


def get_delta_apen(ts): # time-series is an array
    # Perform discrete wavelet transform to get D2 coefficient time series,
    # and create corresponding surrogate time series
    cD2 = pywt.downcoef('d', ts, 'db4', level=2)
    cD2_surrogate = generate_surrogate_series(cD2)
    app_entropy_sample = entropy.app_entropy(cD2, order=2, metric='chebyshev')
    app_entropy_surrogate = entropy.app_entropy(cD2_surrogate, order=2, metric='chebyshev')

    # Return the delta
    delta_ApEns = app_entropy_surrogate - app_entropy_sample
    return delta_ApEns.item()


postgres_properties = {
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD']
}


def postgres_batch_raw(df, epoch_id):
    # foreachBatch write sink; helper for writing streaming dataFrames
    df.write.jdbc(
        url="jdbc:postgresql://ip-10-0-1-33.ec2.internal:5432/speegs",
        table="eeg_data",
        mode="append",
        properties=postgres_properties)

def postgres_batch_analyzed(df, epoch_id):
    # foreachBatch write sink; helper for writing streaming dataFrames
    df.write.jdbc(
        url="jdbc:postgresql://ip-10-0-1-32.ec2.internal:5432/speegs",
        table="eeg_analysis",
        mode="append",
        properties=postgres_properties)


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

    # Parse this into a schema: channel from key, instrument timestamp and voltage from value
    # You can parson JSON using DataFrame functions
    # https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.get_json_object
    dfstreamStr = dfstream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    dfParse = dfstreamStr.select(
        get_json_object(dfstreamStr.key, "$.subject").cast(StringType()).alias("subject_id"),
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
        .groupby(window(col("instr_time"), "16 seconds", "4 seconds"), "subject_id", "channel") \
        .agg(collect_list(struct("instr_time", "voltage")).alias("time_series"))

    # Help function that will sort the grouped time series
    # https://stackoverflow.com/questions/46580253/collect-list-by-preserving-order-based-on-another-variable
    def sort_and_analyze_time_series(l):
        if len(l) < 4096:
            return None

        res = sorted(l, key=operator.itemgetter(0))
        time_series = [item[1] for item in res]
        seizure_indicator = get_delta_apen(time_series)
        return seizure_indicator

    analyze_udf = udf(sort_and_analyze_time_series)
    count_udf = udf(lambda x: len(x))

    # Apply the UDF to the time time series of voltage and obtain the seizure metric
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
        .trigger(processingTime="4 seconds") \
        .start()

    # for dfAnalysis, write as soon as they become available
    dfAnalysisWrite = dfAnalysisFiltered.writeStream \
        .outputMode("append") \
        .foreachBatch(postgres_batch_analyzed) \
        .start()

    dfRawWrite.awaitTermination()
    dfAnalysisWrite.awaitTermination()
