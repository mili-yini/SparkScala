package prod

import java.util.Date
import javax.naming.Context

import Component.HBaseUtil.HbashBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import pipeline.CompositeDoc

/**
  * Created by sunhaochuan on 2016/12/28.
  */
object HBaseDBExtraction {
  def main(args:Array[String]) : Unit = {
    var masterUrl = "local[2]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val sparkConf = new SparkConf()
    val sc = new SparkContext(masterUrl, "SparkHBaseDBExtraction", sparkConf)

    var output_path = "/data/overseas_in/recommendation/galaxy/temp"
    if (args.length > 1) {
      output_path = args(1)
    }
    var freshThreshold : Long = 0;
    if (args.length > 2) {
      freshThreshold = scala.util.Try(args(2).toLong).get
    }
    var tableName = "GalaxyContent"
    if (args.length > 3) {
      tableName = args(3);
    }

    var family = "info"
    if (args.length > 4){
      family = args(4)
    }
    var column = "content"
    if (args.length > 5) {
      column = args(5)
    }

    var startRow :String = null
    if (args.length > 6) {
      startRow = args(6)
    }
    var stopRow :String = null;
    if (args.length > 7) {
      stopRow = args(7)
    }

    val now = new Date();
    var now_timestamp : Long = now.getTime();
    val context: Context = null;
    val hbaseRDD : RDD[(String, String)] = HbashBatch.BatchReadHBaseToRDD(tableName, family, column, sc, startRow, stopRow).filter((line => {
      val doc: CompositeDoc = DocProcess.CompositeDocSerialize.DeSerialize(line._2, context)
      freshThreshold == 0 || (now_timestamp - doc.media_doc_info.crawler_timestamp) < freshThreshold
    }))


    hbaseRDD.saveAsTextFile(output_path)
  }
}
