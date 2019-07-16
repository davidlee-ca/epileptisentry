#!/bin/bash

spark-submit --master spark://ip-10-0-1-33.ec2.internal:7077 \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 \
	--driver-class-path /usr/local/spark/jars/postgresql-42.2.6.jar \
	--jars /usr/local/spark/jars/postgresql-42.2.6.jar \
	--driver-memory 4G \
	--executor-memory 6G \
	v3-calculate-indicators.py 