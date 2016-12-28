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

    var startRow :String = null
    if (args.length > 4) {
      startRow = args(4)
    }
    var stopRow :String = null;
    if (args.length > 5) {
      stopRow = args(5)
    }

    var hbaseRDD : RDD[(String, String)] = HbashBatch.BatchReadHBaseToRDD(tableName, family, column, sc, startRow, stopRow)

    var output_path = "/data/overseas_in/recommendation/galaxy/temp"
    if (args.length > 2) {
      output_path = args(2)
    }
    hbaseRDD.saveAsTextFile(output_path)
  }
}
