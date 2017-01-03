package org.trex.spark2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object FileLineCount {

  def main(args: Array[String]) {


    val sc = SparkSession.builder().appName("orc format file").getOrCreate()
    val file = sc.sparkContext.textFile("hdfs://master:9000/user/hosts")

    println("------------------------------------------------------------------------------")
    println(file.count())
    println(file.first)
    println("------------------------------------------------------------------------------")
  }
}
