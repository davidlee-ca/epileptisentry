# there will be something here
import psycopg2
from pyspark.sql import *
from time import time


if __name__ == "__main__":

    sc = SparkContext(appName="TestDatabaseWrite").getOrCreate()
    sc.setLogLevel("WARN")
    spark = SparkSession(sc)

    rawdata1 = Row(subject='chb01', channel='FP1-F3', timestamp=time()+1, v=3.141592)
    rawdata2 = Row(subject='chb01', channel='FP1-F3', timestamp=time()+2, v=3.141592)
    rawdata3 = Row(subject='chb01', channel='FP1-F3', timestamp=time()+3, v=3.141592)

    df_raw = spark.createDataFrame([rawdata1, rawdata2, rawdata3])
    df_raw.show()
"""
    df_raw.write \
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