package prod

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
import component.HBaseUtil.HbashBatch
import net.sf.json.JSONObject
//import Component.nlp.Text

/**
  * Created by jiaokeke1 on 2016/12/28.
  */
object Batch {
  def main(args:Array[String]) : Unit = {
    val mongoConfig = new Configuration()
    mongoConfig.set("mongo.input.uri",
      "mongodb://10.154.156.118:27017/galaxy.content_access")
    val sparkConf = new SparkConf()
    val sc = new SparkContext("local", "SparkExample", sparkConf)

    val documents = sc.newAPIHadoopRDD(
      mongoConfig,                // Configuration
      classOf[MongoInputFormat],  // InputFormat
      classOf[Object],            // Key type
      classOf[BSONObject])// Value type

    var tableName = "PageViewStream"
    if (args.length > 4) {
      tableName = args(4);
    }

    var processedRDD = documents.map(line => {
      var doc: CompositeDoc = DocumentAdapter.FromJsonStringToCompositeDoc(line._2.toString());
      var serialized_string: String = null;
      var id :String = null;
      if (doc != null) {
        var context: Context = null;
        serialized_string = DocProcess.CompositeDocSerialize.Serialize(doc, context);
        id = doc.media_doc_info.id
      } else {
        System.err.println("Failed to parse :" + line._2)
      }
      (id, serialized_string)
    }).filter( x  => x._1 != null && x._2 != null)
    HbashBatch.BatchWriteToHBase(processedRDD, tableName, "info", "content")


  }
}
