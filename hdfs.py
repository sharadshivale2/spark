import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("hdfs_trial").getOrCreate()
df=spark.read.format("csv").option ("header","True").load("hdfs://localhost:9000/user/data/Fire*")
df.select(df.columns).write.save("/home/administrator/data.csv")
