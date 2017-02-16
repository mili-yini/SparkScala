package prod

import java.util.Date
import javax.naming.Context

import Component.DocumentProcess.{DocumentProcess, GetDoc2VecInput, HotDataTagging}
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
    val need_merge = (args.length > 6) match {
      case true => if (args(6) == "BUILD_INDEX") true else false;
      case false => false
    }
    val need_doc2vec = (args.length > 7) match {
      case true => if (args(7) == "BUILD_DOC2VEC") true else false;
      case false => false
    }
    val feature_path = (args.length > 8) match {
      case true => args(8)
      case false => null
    }
    val startRow:String=(args.length > 9) match {
      case true=> args(9)
      case false=> null
    }

    val stopRow :String =(args.length > 10) match {
      case true=> args(10)
      case false=> null
    }

    val now = new Date();
    var now_timestamp : Long = now.getTime() / 1000;
    val context: Context = null;
    val compositeDoc=HbashBatch.BatchReadHBaseToRDD(tableName, family, column, sc, startRow, stopRow)
      .map(e=>DocProcess.CompositeDocSerialize.DeSerialize(e._2, null))
      .filter(e=>((freshThreshold==0)||(now_timestamp-e.media_doc_info.crawler_timestamp)<freshThreshold))
      .cache()
    //println("Debug Count "+compositeDoc.count())
    //println("now_timestamp " + now_timestamp.toString)

    var mergeLDA=compositeDoc
    val outputPath_LDA_Word2Vector = ""
    if (need_merge && feature_path != null) {
      // this function is used to merge all the feature
      mergeLDA=MergeNlpFeature.mergeLDAFeature(compositeDoc,feature_path + "//*")
      // hot data generation
      val hotTaggedRDD = HotDataTagging.ProcessByMatchHotTag(mergeLDA)
      // serialize the data
      hotTaggedRDD
        .map(e=>(e.media_doc_info.id,DocProcess.CompositeDocSerialize.Serialize(e, context)))
        .saveAsTextFile(output_path + "//aggregate_output") //this location is used to build index
    }

    if (need_doc2vec) {
      val doc2vec_articles = {
        GetDoc2VecInput.Process(compositeDoc)
      }
      doc2vec_articles.saveAsTextFile(output_path + "//articles")
    }

  }
}
