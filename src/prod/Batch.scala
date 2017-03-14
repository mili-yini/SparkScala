package prod

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

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
import Component.DocumentProcess.{DocumentProcess, FastTextHeadlineTag}
import net.sf.json.JSONObject
//import Component.nlp.Text

/**
  * Created by jiaokeke1 on 2016/12/28.
  */
object Batch {
  def main(args:Array[String]) : Unit = {
    var masterUrl = "local[2]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

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
    var time_limit = "";
    var time_limit_int = 0;
    if (args.length > 7) {
      time_limit = args(7)
      try {
        time_limit_int = java.lang.Integer.parseInt(time_limit)
      }
    }
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd  HH:mm:ss")
    val mongoConfig = new Configuration()

    mongoConfig.set("mongo.auth.uri",
      "mongodb://admin:118%23letv.2017@10.154.156.118:27017/admin")
    mongoConfig.set("mongo.input.uri",
      "mongodb://10.154.156.118:27017/galaxy.content_access_new");
    if (time_limit != null && !time_limit.isEmpty && time_limit_int != 0) {
      var date  = new java.util.Date();
      val calendar   =   new   GregorianCalendar();
      calendar.setTime(date);
      calendar.add(Calendar.HOUR, 0 - time_limit_int);//把日期往后增加一天.整数往后推,负数往前移动
      date=calendar.getTime();   //这个时间就是日期往后推一天的结果
      val time_string = simpleDateFormat.format(date)
      val input_query = "{\"crawl_time\":{\"$gte\" : \"" + time_string + "\"}}";
      mongoConfig.set("mongo.input.query", input_query);
      System.out.println("input, query: " + input_query)
    }

    //mongoConfig.set("mongo.input.query", "{\"info_id\":\"212_8051031210230967516\"}");

    val sparkConf = new SparkConf() //.setMaster(masterUrl).setAppName("ProdBatch")
    val sc = new SparkContext(masterUrl, "ProdBatch", sparkConf)

    val documents = sc.newAPIHadoopRDD(
      mongoConfig,                // Configuration
      classOf[MongoInputFormat],  // InputFormat
      classOf[Object],            // Key type
      classOf[BSONObject])// Value type

    //println("S "+documents.count())


    val user_dict = DocumentProcess.UserDictPrepare(sc)
    val fast_text_library = FastTextHeadlineTag.FastTextPrepare(sc)
    val processedRDD = DocumentProcess.ProcessBatch(documents, fast_text_library._1, fast_text_library._2)
    HbashBatch.BatchWriteToHBaseWithDesignRowkey(processedRDD, tableName, family, column,
      mappingTableName, mappingFamily, mappingColumn)
    //processedRDD.foreach(e=>println(e))
    //println("E "+processedRDD.count())

  }
}
