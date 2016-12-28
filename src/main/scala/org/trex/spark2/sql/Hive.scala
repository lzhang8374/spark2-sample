package org.trex.spark2.sql

import org.apache.spark.sql.{Row, SparkSession}

/**
  * Spark sql 操作hive
  * 1、拷贝3个文件到spark配置文件目录conf中
  *    hive-site.xml, core-site.xml, hdfs-site.xml
  * 2、
  */
object Hive {

  case class Record(key: Int, value: String)

  def main(args: Array[String]) {


    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
    .builder()
    .appName("Spark Hive")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()

    sql("SELECT COUNT(*) FROM src").show()

    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    // The items in DaraFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
  }
}
