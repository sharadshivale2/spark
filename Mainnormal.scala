package og.sharad.examples

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object Sales 
{

def main(args:Array[String]):Unit=
{

val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .getOrCreate()

//val df = spark.read.format("csv").option("header","True").option("inferschema","True").load("/home/administrator/vgsales.csv")
val df1 = spark.read.format("csv").option("header","True").option("inferschema","True").load("hdfs://localhost:9000/user/data/Sales.csv")
df1.show(2)
//df.createOrReplaceTempView("data")
df1.createOrReplaceTempView("data")
val sqlDf = spark.sql("SELECT * FROM data").show(5)
//spark.sql("select sum(Global_Sales) from data where Name like '%Pokemon%'").show

//spark.sql("select Country,to_date(`Order Date`,'M/d/yyyy')as date,`Total Profit` from data ").filter("date>'2014-01-01' and date<'2014-01-31'").show

//val data =df.write.format("com.databricks.spark.avro").save("/home/administrator/sher/t2.avro")
//val data =sqlDf.write.format("com.databricks.spark.avro").save("hdfs://192.168.0.94:9000/user/data/t2.avro")

//val sh= spark.read.format("com.databricks.spark.avro").option("header","True").load("/home/administrator/sher/t2.avro/part*")
//sh.show(2)
/*
val schema1 = StructType(
    StructField("f1", StringType)::
    StructField("f2", StringType)::
    StructField("f3", DoubleType)::
    StructField("f4", StringType)::
    StructField("f5", DoubleType)::
    StructField("f6", DoubleType)::
    StructField("f7", DoubleType)::
    StructField("f8", DoubleType)::
    StructField("f9", DoubleType)::
    StructField("f10", StringType)::
    StructField("f11", StringType)::
    StructField("f12", StringType)::
    StructField("f13", StringType)::
    StructField("f14", StringType)::
    StructField("f15", StringType)::
    StructField("f16", StringType)::
    StructField("f17", StringType)::
    StructField("f18", StringType)::
    StructField("f19", StringType)::
    StructField("f20", StringType)::
    StructField("f21", StringType)::
    StructField("f22", StringType)::Nil)

val df =spark.read.format("csv").option("delimiter","\t").option("inferSchema","True").schema(schema1).load("/home/administrator/youtubedata.txt")
df.show(2)

*/

}
}
