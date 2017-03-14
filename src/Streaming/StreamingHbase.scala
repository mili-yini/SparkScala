package Streaming
import java.text.SimpleDateFormat
import java.util.Date

import Component.HBaseUtil.{HbashBatch, HiveBatch}
import Component.Util.JNACall.CLibrary
import Component.Util.{JNACall, SocketClient, ZhCnWordProcess}
import DocProcess.CompositeDocSerialize
import com.letv.scheduler.thrift.core.ImageTextDoc
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkFiles}
import net.sf.json.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by sunhaochuan on 2017/3/9.
  */
object StreamingHbase {

  def main(args: Array[String]): Unit = {
    // Create a StreamingContext with the given master URL
    var masterUrl = "local[2]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val conf = new SparkConf().setMaster(masterUrl).setAppName("TestHbaseStream")
    val ssc = new StreamingContext(conf, Seconds(30))

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
    val zookeeper = "10.121.145.129,10.121.145.157,10.121.145.130"
    val tableName = "GalaxyText"
    val family = "value"
    val column = "content"

    // Create streaw from hdfs
    val hdfsStream = ssc.textFileStream("/data/search/short_video/imagetextdoc/output/inc/galaxy/")

    println("Start to streaming")
    hdfsStream.foreachRDD(
      rdd => {
        val split_rdd = rdd.map( line => {
          //val imageTextDoc : ImageTextDoc = CompositeDocSerialize.DeSerializeImageTextDoc(line)
          val arr : Array[String] = line.split("\t", 3)
          (arr(0), arr(2))
        })
        //HbashBatch.BatchWriteToHbaseWithConfig(split_rdd, zookeeper, tableName, family, column)
        HiveBatch.WriteToHiveKVTable(split_rdd, "tmp.test_a_b")
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
