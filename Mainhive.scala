package or.sher.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
object Hive {
  def main(args: Array[String]) {
val spark=SparkSession.builder.master("local")
			.appName("spark session example")
			.config("spark.sql.warehouse.dir","/user/hive/warehouse")
			.config("hive.metastore.uris","thrift://localhost:9083")
			.enableHiveSupport( ).getOrCreate()
spark.sql("use russ")
spark.sql("create table emp(int id,name string, dept string,yoj int)row format delimited fields terminated by `,` lines terminated by `\n stored as textfile")
spark.sql("load data inpath '/home/administrator/data.txt' into table emp")
val df=spark.sql("select * from emp")
df.show()
  }
}