package prod

import Component.HBaseUtil.HbashBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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

    var tableName = "PageViewStream"
    if (args.length > 1) {
      tableName = args(1);
    }

    var hbaseRDD : RDD[(String, String)] = HbashBatch.BatchReadHBaseToRDD(tableName, "info", "content", sc)

    var output_path = "/data/overseas_in/recommendation/galaxy/temp"
    if (args.length > 1) {
      output_path = args(1)
    }
    hbaseRDD.saveAsTextFile(output_path)
  }
}
