package Streaming

import java.text.SimpleDateFormat
import java.util.Date

import Component.Util.JNACall.CLibrary
import Component.Util.{JNACall, SocketClient, ZhCnWordProcess}
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkFiles}
import net.sf.json.JSONObject
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by sunhaochuan on 2016/12/16.
  */
object KafkaStreaming {
  def main(args: Array[String]): Unit = {

    // Create a StreamingContext with the given master URL
    var masterUrl = "local[2]"
    val conf = new SparkConf().setMaster(masterUrl).setAppName("ProdStream")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    var topics: Set[String] = null;
    if (args.length > 1) {
      topics = Set(args(1)) //table name
    } else {
      topics = Set("test_content");
    }
    //本地虚拟机ZK地址
    var brokers = "10.121.145.144:9092";
    if (args.length > 2) {
      brokers = args(2);
    }
    var zookeeper_location = "10.121.145.24:2181,10.121.145.26:2181,10.121.145.25:2181";
    if (args.length > 3) {
      zookeeper_location = args(3);
    }
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "zookeeper.connect" -> zookeeper_location,
      "group.id" -> "spark-streaming-test",
      "zookeeper.connection.timeout.ms" -> "30000")

    CLibrary.INSTANCE.LoadModel("model.bin", 0)
    val broadcastInstance = ssc.sparkContext.broadcast(CLibrary.INSTANCE)
    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    println("Start to streaming")
    kafkaStream.foreachRDD(
      rdd => {
        //val sc = rdd.sparkContext
        //val broadcastInstance = sc.broadcast(CLibrary.INSTANCE)

        //sc.addFile("libfasttext.so")
        //SparkFiles.get("libfasttext.so")

        val a = rdd.map(line => {
          val instance : JNACall.CLibrary = broadcastInstance.value
          var scoket = new SocketClient()
          //var res = "";//scoket.OutBandProcessBySocket(line._2.toString())
          val res: String = instance.Predict(ZhCnWordProcess.SplitZHCN(line._2.toString), 10, 0)
          (line._2, res)
        }
       )
       //a.sparkContext.addFile()
       a.foreach(line=> {
         println(line._1 + ":" + line._2)
       })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
