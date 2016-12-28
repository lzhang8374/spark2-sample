package org.trex.spark2.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by lzhang on 16-12-27.
  */
object DataFrame {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("spark sql").master("local[1]").getOrCreate()


    val df = spark.read.json("/home/lzhang/development/workspace/samples/spark2-sample/src/main/resources/data/sql/people.json")

    // This import is needed to use the $-notation
    import spark.implicits._

    // 显示前10行数据
    df.show(10)
    // 打印表结构
    df.printSchema()
    // 选择 "name" 列
    df.select("name").show()
    // 所有的年龄加1
    df.select($"name", $"age" + 1).show()
    // 选择年龄大于21的记录
    df.filter($"age" > 21).show()
    // 按年龄统计数量
    df.groupBy("age").count().show()

    // 注册成表
    df.createOrReplaceTempView("people")
    // 查询表
    val sqlDF = spark.sql("SELECT * FROM people where name = 'zhanglei'")
    sqlDF.show()
  }
}
