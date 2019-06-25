from pyspark import SparkContext
from pyspark.sql import *
from time import time
from datetime import datetime as dt
import os



postgres_url = "jdbc:postgresql://ec2-3-215-187-200.compute-1.amazonaws.com:5432/speegs"
properties = {
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD']
}

if __name__ == "__main__":

    sc = SparkContext(appName="TestDatabaseWrite").getOrCreate()
    sc.setLogLevel("WARN")
    spark = SparkSession(sc)

    rawdata1 = Row(subject_id='chb01', channel='FP1-F3', instrument_time=dt.fromtimestamp(time()+1), reading=3.141592)
    rawdata2 = Row(subject_id='chb01', channel='FP1-F3', instrument_time=dt.fromtimestamp(time()+2), reading=3.141592)
    rawdata3 = Row(subject_id='chb01', channel='FP1-F3', instrument_time=dt.fromtimestamp(time()+3), reading=3.141592)

    df_raw = spark.createDataFrame([rawdata1, rawdata2, rawdata3])
    df_raw.show()

    df_raw.write.jdbc(url=postgres_url, table="eeg_data", mode="append", properties=properties)

"""

    df_raw.write.jdbc \
        .format("jdbc") \
        .option("url", "jdbc:ec2-3-215-187-200.compute-1.amazonaws.com:5432") \
        .option("dbtable", "speegs.eeg_data") \
        .option("user", "postgres") \
        .option("password", "insightData") \
        .save()
"""
"""
CREATE TABLE IF NOT EXISTS "eeg_data"(
    instrument_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    subject_id TEXT,
    channel TEXT,
    reading REAL
);
"""
