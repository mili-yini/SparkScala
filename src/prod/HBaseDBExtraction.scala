package prod

import java.util.Date
import javax.naming.Context

import Component.HBaseUtil.HbashBatch
import Component.nlp.{MergeNlpFeature, Word2Vector}
import ldacore.CalLDA
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import pipeline.CompositeDoc

/**
  * Created by sunhaochuan on 2016/12/28.
  */
object HBaseDBExtraction {
  def main(args:Array[String]) : Unit = {
    val masterUrl = (args.length > 0) match{
      case true=> args(0)
      case false=>"local[2]"
    }
    val sparkConf = new SparkConf()
    val sc = new SparkContext(masterUrl, "SparkHBaseDBExtraction", sparkConf)

    val output_path = (args.length > 1) match {
      case true=>args(1)
      case false=>"/data/overseas_in/recommendation/galaxy/temp"
    }

    val freshThreshold=(args.length > 2) match{
      case true=>args(2).toLong
      case false=>0
    }
    val tableName=(args.length > 3) match {
      case true=> args(3)
      case false=> "GalaxyContent"
    }
    val family=(args.length > 4) match {
      case true=> args(4)
      case false=> "info"
    }
    val column=(args.length > 5) match {
      case true=> args(5)
      case false=> "content"
    }
    val startRow:String=(args.length > 6) match {
      case true=> args(6)
      case false=> null
    }

    val stopRow :String =(args.length > 7) match {
      case true=> args(7)
      case false=> null
    }

    val now = new Date();
    var now_timestamp : Long = now.getTime();
    val context: Context = null;
    val compositeDoc=HbashBatch.BatchReadHBaseToRDD(tableName, family, column, sc, startRow, stopRow)
      .map(e=>DocProcess.CompositeDocSerialize.DeSerialize(e._2, null))
      .filter(e=>((freshThreshold==0)||(now_timestamp-e.media_doc_info.crawler_timestamp)<freshThreshold))
      .cache()


   //add by lujing
    val flag=true
    val outputPath_LDA_Word2Vector = ""
    var mergeLDA=compositeDoc
    if(flag) {
      Word2Vector.getEntityRelation(compositeDoc, outputPath_LDA_Word2Vector + "wordRelation")
      MergeNlpFeature.calLDAFeature(compositeDoc, outputPath_LDA_Word2Vector + "LDA")
    }else{
      mergeLDA=MergeNlpFeature.mergeLDAFeature(compositeDoc,outputPath_LDA_Word2Vector + "LDA")
    }
    mergeLDA
      .map(e=>(e.media_doc_info.id,DocProcess.CompositeDocSerialize.Serialize(e, context)))
      .saveAsTextFile(output_path)
  }
}
