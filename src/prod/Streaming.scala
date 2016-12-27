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
    val conf = new SparkConf().setMaster(masterUrl).setAppName("ProdStream")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    var topics: Set[String] = null;
    if (args.length > 1) {
      topics = Set(args(1)) //table name
    } else {
      topics = Set("prod_content");
    }
    //本地虚拟机ZK地址
    var brokers = "10.121.145.144:9092";
    if (args.length > 2) {
      brokers = args(2);
    }
    var zookeeper_location = "10.121.145.27:2181,10.121.145.26:2181,10.121.145.25:2181";
    if (args.length > 3) {
      zookeeper_location = args(3);
    }
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "zookeeper.connect" -> zookeeper_location,
       "group.id" -> "spark-streaming-test",
      "zookeeper.connection.timeout.ms" -> "30000")

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

     kafkaStream.foreachRDD(rdd => {
       rdd.foreach(line => {
         println(line._2)
         var tableName = "PageViewStream"
         if (args.length > 4) {
           tableName = args(4);
         }
         var rowKey = "test";
         HbaseTool.putValue(tableName, rowKey, "info", Array(("id", line._2)))


       })
     })

    ssc.start()
    ssc.awaitTermination()

  }
}
