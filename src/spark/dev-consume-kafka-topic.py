# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession


if __name__ == "__main__":

    sc = SparkContext(appName="PySparkReadFromKafka")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5) # 5 seconds interval

    spark =
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
        .option("subscribe", "topic1") \
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")



    df.pprint()

    ssc.start()
    ssc.awaitTermination()