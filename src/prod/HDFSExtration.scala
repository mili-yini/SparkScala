package prod

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import javax.naming.Context

import Component.DocumentProcess.{GetDoc2VecInput, HotDataTagging}
import Component.HBaseUtil.HbashBatch
import Component.nlp.MergeNlpFeature
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sunhaochuan on 2017/4/18.
  */
object HDFSExtration {
  def main(args:Array[String]) : Unit = {
    val masterUrl = (args.length > 0) match{
      case true=> args(0)
      case false=>"local[2]"
    }
    val sparkConf = new SparkConf()
    val sc = new SparkContext(masterUrl, "SparkHDFSExtraction", sparkConf)

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
    val context = null

    val today_format = new SimpleDateFormat("yyyy-MM-dd_").format(new Date())
    val compositeDoc_inc1 = sc.textFile("/data/rec/recommendation/galaxy/doc_process/streaming/" + today_format + "*/part*")
    println("Today Count "+compositeDoc_inc1.count())
    val calendar = Calendar.getInstance()
    calendar.setTime(new Date())
    calendar.add(Calendar.DAY_OF_MONTH, -1);
    val yesterday_format = new SimpleDateFormat("yyyy-MM-dd_").format(calendar.getTime)
    val compositeDoc_inc2 = sc.textFile("/data/rec/recommendation/galaxy/doc_process/streaming/" + yesterday_format + "*/part*")
    println("Yesterday Count "+compositeDoc_inc2.count())

    var docs = compositeDoc_inc1.union(compositeDoc_inc2)

    if (tableName == "Batch") {
      docs = docs.union(sc.textFile("/data/rec/recommendation/galaxy/doc_process/batch/data"))
      docs = docs.distinct()
    }
    val compositeDoc = docs.map(e=>e.split(",")).filter(_.length == 3)
      .map(e=>DocProcess.CompositeDocSerialize.DeSerialize(e(1), null))
      .filter(e=>((freshThreshold==0)||(now_timestamp-e.media_doc_info.crawler_timestamp)<freshThreshold))
      .filter(e=>(e.play_mark == 1)) // used by cms
      .cache()
    val compositeDoc_count = compositeDoc.count()
    println("Debug Count " + compositeDoc_count)
    if (compositeDoc_count < 100000) {
      return
    }
    println("now_timestamp " + now_timestamp.toString)

    var mergeLDA=compositeDoc
    val outputPath_LDA_Word2Vector = ""
    if (need_merge && feature_path != null) {
      // this function is used to merge all the feature
      //mergeLDA=MergeNlpFeature.mergeLDAFeature(compositeDoc,feature_path + "//*")
      // merge fasttext tag
      val mergeFasttext = MergeNlpFeature.mergeFastTexFeature(compositeDoc, "/data/search/short_video/imagetextdoc/parser/galaxy/content_tag/*")

      /*mergeFasttext.map(e=>{
        var tag_str:String = ""
        for(tag<-e.body_nnp) {
          tag_str = tag_str + " " + tag
        }
        (e.media_doc_info.id, e.media_doc_info.play_url, e.media_doc_info.name, tag_str)
      }).saveAsTextFile(output_path + "//tag_dump")*/

      // hot data generation
      val broadcase_sm = HotDataTagging.FastTextPrepare(sc)
      val hotTaggedRDD = HotDataTagging.ProcessByMatchHotTag(mergeFasttext, broadcase_sm)
      // serialize the data
      hotTaggedRDD
        .map(e=>(e.media_doc_info.id,DocProcess.CompositeDocSerialize.Serialize(e, context)))
        .saveAsTextFile(output_path + "//aggregate_output") //this location is used to build index
    }


  }
}
