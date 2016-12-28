package org.trex.spark2.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by lzhang on 16-12-27.
  */
object Test1 {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("spark sql").master("local[1]").getOrCreate()

    // 创建表结构
    val schemaString = "name age"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // 创建数据
    val peopleRDD = spark.sparkContext.textFile("/home/lzhang/development/workspace/samples/spark2-sample/src/main/resources/data/sql/people.txt")
    val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))

    // 创建dataframe
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // 注册为表
    peopleDF.createOrReplaceTempView("people")

    // 查询表，返回dataframe
    val results = spark.sql("SELECT name FROM people")

    results.show()

    // dataframe在做map操作
    //results.map(attributes => "Name: " + attributes(0)).show()
  }

}
