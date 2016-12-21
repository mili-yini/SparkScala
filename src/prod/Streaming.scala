package prod

import Component.HBaseUtil.HbaseTool
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by sunhaochuan on 2016/12/16.
  */
object Streaming {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("TestStream")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    val topics = Set("kafka-test02")
    //本地虚拟机ZK地址
    val brokers = "10.121.145.144:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "zookeeper.connect" -> "10.121.145.144:2181",
       "group.id" -> "spark-streaming-test",
    "zookeeper.connection.timeout.ms" -> "30000")

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

     kafkaStream.foreachRDD(rdd => {
       rdd.foreach{line=> {
         println(line._2)
         val tableName = "PageViewStream"
         HbaseTool.putValue(tableName, "ID12345", "info", Array(("id", line._2)))
//         val s=HbaseTool.getValue(tableName,"1","info",Array("id"))
//         println(s)
       }
       }

    })

    ssc.start()
    ssc.awaitTermination()

  }
}
