package org.trex.spark2.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by lzhang on 16-12-27.
  */
object Test2 {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("spark sql").master("local[1]").getOrCreate()


    val usersDF = spark.read.format("parquet").load("/home/lzhang/development/workspace/samples/spark2-sample/src/main/resources/data/sql/users.parquet")

    usersDF.printSchema()
    usersDF.show()

    usersDF.select("name", "favorite_color").write.format("json").save("/home/lzhang/development/workspace/samples/spark2-sample/src/main/resources/data/sql/newusers.json")
  }
}
