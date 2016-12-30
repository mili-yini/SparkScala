package prod

import java.text.SimpleDateFormat
import java.util.Date

import Component.HBaseUtil.HbaseTool
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import CompositeDocProcess.DocumentAdapter
import pipeline.CompositeDoc
import javax.naming.Context

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import com.mongodb.hadoop.{BSONFileInputFormat, BSONFileOutputFormat, MongoInputFormat, MongoOutputFormat}
import com.mongodb.hadoop.io.MongoUpdateWritable
import Component.HBaseUtil.HbashBatch
import Component.DocumentProcess.DocumentProcess
import net.sf.json.JSONObject
//import Component.nlp.Text

/**
  * Created by jiaokeke1 on 2016/12/28.
  */
object Batch {
  def main(args:Array[String]) : Unit = {
    var masterUrl = "local"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val mongoConfig = new Configuration()
    mongoConfig.set("mongo.input.uri",
      "mongodb://10.154.156.118:27017/galaxy.content_access")
    val sparkConf = new SparkConf() //.setMaster(masterUrl).setAppName("ProdBatch")
    val sc = new SparkContext(masterUrl, "ProdBatch", sparkConf)

    val documents = sc.newAPIHadoopRDD(
      mongoConfig,                // Configuration
      classOf[MongoInputFormat],  // InputFormat
      classOf[Object],            // Key type
      classOf[BSONObject])// Value type

    var tableName = "GalaxyContent"
    if (args.length > 1) {
      tableName = args(1);
    }
    var family = "info"
    if (args.length > 2){
      family = args(2)
    }
    var column = "content"
    if (args.length > 3) {
      column = args(3)
    }
    var mappingTableName = "GalaxyKeyMapping"
    if (args.length > 4) {
      mappingTableName = args(4)
    }
    var mappingFamily = "info"
    if (args.length > 5){
      mappingFamily = args(5)
    }
    var mappingColumn = "OriginalKey"
    if (args.length > 6) {
      mappingColumn = args(6)
    }
    //println("S "+documents.count())
    val processedRDD = DocumentProcess.ProcessBatch(documents)
    HbashBatch.BatchWriteToHBaseWithDesignRowkey(processedRDD, tableName, family, column,
      mappingTableName, mappingFamily, mappingColumn)
    //processedRDD.foreach(e=>println(e))
    //println("E "+processedRDD.count())

  }
}
