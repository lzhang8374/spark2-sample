package org.trex.spark2.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by lzhang on 16-12-27.
  */
object DataSet {


  def main(args: Array[String]) {
//    val spark = SparkSession.builder().appName("spark sql").master("local[1]").getOrCreate();
//    import spark.implicits._
//
//    case class Person(name: String, age: Long)
//
//
//    // Encoders are created for case classes
//    val caseClassDS = Seq(Person("Andy", 32)).toDS()
//    caseClassDS.show()
//
//    // Encoders for most common types are automatically provided by importing spark.implicits._
//    val primitiveDS = Seq(1, 2, 3).toDS()
//    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
//
//    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
//    val path = "examples/src/main/resources/people.json"
//    val peopleDS = spark.read.json(path).as[Person]
//    peopleDS.show()
  }
}
