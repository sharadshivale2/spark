import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.appName("Youtube_Data_Analysis").getOrCreate()
schema1 = StructType([
    StructField("f1", StringType(), True),
    StructField("f2", StringType(), True),
    StructField("f3", DoubleType(), True),
    StructField("f4", StringType(), True),
    StructField("f5", DoubleType(), True),
    StructField("f6", DoubleType(), True),
    StructField("f7", DoubleType(), True),
    StructField("f8", DoubleType(), True),
    StructField("f9", DoubleType(), True),
    StructField("f10", StringType(), True),
    StructField("f11", StringType(), True),
    StructField("f12", StringType(), True),
    StructField("f13", StringType(), True),
    StructField("f14", StringType(), True),
    StructField("f15", StringType(), True),
    StructField("f16", StringType(), True),
    StructField("f17", StringType(), True),
    StructField("f18", StringType(), True),
    StructField("f19", StringType(), True),
    StructField("f20", StringType(), True),
    StructField("f21", StringType(), True),
    StructField("f22", StringType(), True),
    StructField("f23", StringType(), True)])
df_read = spark.read \
 .option("delimiter", "\t") \
 .schema(schema1) \
 .option("inferSchema", "True") \
 .csv("/home/administrator/youtubedata.txt")
df_read.createOrReplaceTempView("youtube_data") # create temporary table
df_read.printSchema()
df1 = spark.sql('''select * from youtube_data''').show()
#df1.createOrReplaceTempView("col_4")
#df2 = spark.sql('''select vdo_category, COUNT(vdo_category) category_count from col_4 group by vdo_category order by category_count DESC''')
#df2.coalesce(1).write.option("delimiter",",").option("header", "true").mode("overwrite").csv('s3://Bucket_Name/xxxx-xxxx/xxxx/spark/TopCategory')