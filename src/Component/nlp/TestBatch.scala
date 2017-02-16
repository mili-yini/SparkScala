package Component.nlp

import Component.DocumentProcess.DocumentProcess
import Component.HBaseUtil.HbashBatch
import com.mongodb.hadoop.MongoInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject
import scala.collection.JavaConversions._
//import Component.nlp.Text

/**
  * Created by jiaokeke1 on 2016/12/28.
  */
object TestBatch {
  def main(args:Array[String]) : Unit = {
    var masterUrl = "local[2]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val mongoConfig = new Configuration()
    mongoConfig.set("mongo.input.uri",
      "mongodb://10.154.156.118:27017/galaxy.content_access_new")
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
    val processedRDD = DocumentProcess.ProcessBatch(documents)
//    HbashBatch.BatchWriteToHBaseWithDesignRowkey(processedRDD, tableName, family, column,
//      mappingTableName, mappingFamily, mappingColumn)
    val rddBase64=processedRDD
  .map(e=>DocProcess.CompositeDocSerialize.DeSerialize(e._2, null)).cache()

      MergeNlpFeature.calLDAFeature(rddBase64,"D:\\a")
    val result=MergeNlpFeature.mergeLDAFeature(rddBase64,"D:\\a")
    result.filter(e=>e.media_doc_info.getLdavec.size()>0)
      .foreach(e=>println(e.media_doc_info.getLdavec))


  }
}
