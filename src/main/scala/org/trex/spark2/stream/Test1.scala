package org.trex.spark2.stream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by lzhang on 16-12-27.
  */
object Test1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))


    // Kafka configurations
    val topics = Set("spark_test")
    val brokers = "localhost:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

    // Create a direct stream
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val words = lines.flatMap(_._2.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)



    wordCounts.print()


    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
