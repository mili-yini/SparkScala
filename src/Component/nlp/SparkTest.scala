package Component.nlp
/**
  * Created by sunhaochuan on 2017/2/10.
  */

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

object SparkTest {
  def main(args:Array[String]) : Unit = {
    var masterUrl = "local[2]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val mongoConfig = new Configuration()
    mongoConfig.set("mongo.auth.uri",
      "mongodb://admin:118%23letv.2017@10.154.156.118:27017/admin")
    mongoConfig.set("mongo.input.uri",
      "mongodb://10.154.156.118:27017/galaxy.content_access_new");
    val sparkConf = new SparkConf() //.setMaster(masterUrl).setAppName("ProdBatch")
    val sc = new SparkContext(masterUrl, "ProdBatch", sparkConf)

    val documents = sc.newAPIHadoopRDD(
      mongoConfig,                // Configuration
      classOf[MongoInputFormat],  // InputFormat
      classOf[Object],            // Key type
      classOf[BSONObject])// Value type


    val processedRDD = documents.map(line => {
      val doc: CompositeDoc = DocumentAdapter.FromJsonStringToCompositeDoc(line._2.toString);

      var debug_string = "";
      if (doc != null) {
        //add by lujing
        val text=new Text(doc.media_doc_info.name,doc.description)
        text.addComopsticDoc(doc)

        debug_string = "url:" + doc.media_doc_info.play_url + " title: " + doc.media_doc_info.normalized_name + "\n";

        debug_string = debug_string + text.Debug();


      } else {
        System.err.println("Failed to parse :" + line._2)
      }

      debug_string
    })

    processedRDD.foreach(e=>println(e))
    //println("E "+processedRDD.count())

  }
}
