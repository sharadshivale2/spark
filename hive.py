import sys
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
spark=SparkSession.builder.master("local").appName("spark session example").config("spark.sql.warehouse.dir","/user/hive/warehouse").config("hive.metastore.uris","thrift://localhost:9083").enableHiveSupport( ).getOrCreate()
#vg=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/user/data/data.txt")
#vg=spark.read.format("csv").option("header","True").option("inferSchema","True").load("file:///home/administrator/data.txt")
#vg.show()
#vg.createTempView("data")
spark.sql("use russ")
#spark.sql("create table emp4(id int,name string, dept string,yoj int) stored as orc")
#spark.sql("create table emp4(id int,name string, dept string,yoj int) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile")
spark.sql("insert into emp4 values (1,'sher','HR',2012)")
#spark.sql("create database russ")
#spark.sql("use russ")
#spark.sql("create table emp(id int,name string, dept string,yoj int) row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile")
#spark.sql("load data inpath '/user/data/data.txt' overwrite into table emp")
#df=spark.sql("select * from emp")
df=spark.sql("select * from emp4")
df.show()