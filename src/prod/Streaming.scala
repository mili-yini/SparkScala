package prod

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import Component.HBaseUtil.{HbaseTool, HbashBatch}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import CompositeDocProcess.DocumentAdapter
import pipeline.CompositeDoc
import javax.naming.Context

import Component.DocumentProcess.{DocumentProcess, FastTextHeadlineTag}
import Component.Util.JNACall.CLibrary
import Component.Util.{JNACall, StringMatch, ZhCnWordProcess}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import shared.datatypes.{FeatureType, ItemFeature}

import scala.collection.JavaConversions._
import scala.collection.mutable
//import Component.nlp.Text
/**
  * Created by sunhaochuan on 2016/12/16.
  */
object Streaming {

  var broadcastInstance_t : Broadcast[CLibrary] = null
  var broadcastSm_t : Broadcast[StringMatch] = null

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("ProdStream")
    val ssc = new StreamingContext(conf, Seconds(300))

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
    var zookeeper_location = "10.121.145.24:2181,10.121.145.26:2181,10.121.145.25:2181";
    if (args.length > 3) {
      zookeeper_location = args(3);
    }

    var tableName = "GalaxyContent"
    if (args.length > 4) {
      tableName = args(4);
    }
    var family = "info"
    if (args.length > 5){
      family = args(5)
    }
    var column = "content"
    if (args.length > 6) {
      column = args(6)
    }
    var mappingTableName = "GalaxyKeyMapping"
    if (args.length > 7) {
      mappingTableName = args(7)
    }
    var mappingFamily = "info"
    if (args.length > 8){
      mappingFamily = args(8)
    }
    var mappingColumn = "OriginalKey"
    if (args.length > 9) {
      mappingColumn = args(9)
    }

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "zookeeper.connect" -> zookeeper_location,
      "group.id" -> "spark-streaming-test",
      "zookeeper.connection.timeout.ms" -> "30000")

    val sc = ssc.sparkContext

    val fasttext_library = FastTextHeadlineTag.FastTextPrepare(sc)

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    println("Start to streaming")
    kafkaStream.foreachRDD(documents => {

      if (!documents.isEmpty()) {
        val processedRDD = DocumentProcess.ProcessStream(documents, fasttext_library._1, fasttext_library._2)

        val strdate=new SimpleDateFormat("yyyy-MM-dd_HHmmss").format(new Date())
        processedRDD.saveAsTextFile("/data/rec/recommendation/galaxy/doc_process/streaming/" + strdate)
        //val processedRDD1 = ProcessLibrary(processedRDD, broadcastInstance, broadcastSm)
        //HbashBatch.BatchWriteToHBaseWithDesignRowkey(processedRDD, tableName, family, column,
        //  mappingTableName, mappingFamily, mappingColumn)

        // below is used to debug

        /*processedRDD.map(e => {
          val context: Context = null;
          val doc = DocProcess.CompositeDocSerialize.DeSerialize(e._2, context)
          doc
        }).foreach(e=>{
          println("raw input: " + e.classifier_input)
          println("ft_category_raw_output: " + e.title_NER_person.get(0))
          println("ft_tag_raw_output: " + e.title_NER_person.get(1))
        })

        //processedRDD1.map(e=>{(e._1, e._2)}).saveAsHadoopFile("/data/overseas_in/recommendation/pipeline/tmp/test/abc")*/
      } else {
        println("Get the empty data in Streaming")
      }

    })

    ssc.start()
    ssc.awaitTermination()

  }
}
