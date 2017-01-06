package Component.DocumentProcess

import scala.collection.JavaConversions._
import java.text.SimpleDateFormat
import java.util.Date
import javax.naming.Context

import Component.HBaseUtil.HbashBatch
import Component.Util.StringMatch
import Component.nlp.Text
import CompositeDocProcess.DocumentAdapter
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import pipeline.CompositeDoc
import shared.datatypes.{FeatureType, ItemFeature}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
  * Created by sunhaochuan on 2016/12/28.
  */
object DocumentProcess {
  // define the different interface to streaming and batch
  def ProcessStream( documents : RDD[(String, String)]) : RDD[(String, String, String)] = {
    Process(documents)
  }
  def ProcessBatch( documents : RDD[(Object, BSONObject)]) : RDD[(String, String, String)] = {
    Process(documents.map(document => {(document._1.toString(), document._2.toString())}))
  }
  def Process(documents : RDD[(String, String)]) : RDD[(String, String, String)] = {

    val processedRDD = documents.map(line => {
      val doc: CompositeDoc = DocumentAdapter.FromJsonStringToCompositeDoc(line._2);

      //
      var serialized_string: String = null;
      var id :String = null;
      var date_prefix: String = null;
      if (doc != null) {
        //add by lujing
        val text=new Text(doc.media_doc_info.name,doc.description)
        text.addComopsticDoc(doc)

        // add the feature list to media doc info
        doc.feature_list.map(e=>doc.media_doc_info.feature_list.put(e.name, e))
        val context: Context = null;
        serialized_string = DocProcess.CompositeDocSerialize.Serialize(doc, context)
        id = doc.media_doc_info.id
        val dateFormat = new SimpleDateFormat("yyMMdd")
        val crawler_time = new Date(doc.media_doc_info.crawler_timestamp)
        date_prefix = dateFormat.format(doc.media_doc_info.crawler_timestamp * 1000 )

      } else {
        System.err.println("Failed to parse :" + line._2)
      }
      (id, serialized_string, date_prefix)
    }).filter( x  => x._1 != null && x._2 != null)

    processedRDD
  }

  def ProcessByMatchHotTag(documents: RDD[CompositeDoc]) : RDD[CompositeDoc] = {
    val sc = documents.sparkContext

    val input_hot_data_dir = "D:\\Temp\\hot\\"

    val sm : StringMatch = new StringMatch()

    //http://10.148.12.101:8000/wanghongqing/chenglinpeng/leview_movie/out/hot_people
    val list_baidutop_people = sc.textFile(input_hot_data_dir + "hot_people").collect().toList
    //val list_baidutop_movie = sc.textFile("").collect().toList
    //val list_baidutop_tv = sc.textFile("").collect().toList
    //val list_baidutop_variety = sc.textFile("").collect().toList
    //val list_baidutop_music = sc.textFile("").collect().toList
    //val list_baidutop_cartoon = sc.textFile("").collect().toList

    //http://10.148.12.101:8000/wanghongqing/leview_news/data/baidu_today_hotsearch.txt
    val list_headline_baidu_today_hotsearch = sc.textFile(input_hot_data_dir + "baidu_today_hotsearch.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/ifeng_headline.txt
    val list_headline_ifeng = sc.textFile(input_hot_data_dir + "ifeng_headline.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/sina_headline.txt
    val list_headline_sina = sc.textFile(input_hot_data_dir + "sina_headline.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/ifeng_finance.txt
    val list_finance_ifeng = sc.textFile(input_hot_data_dir + "ifeng_finance.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/sina_sport.txt
    val list_sport_sina = sc.textFile(input_hot_data_dir + "sina_sport.txt").collect().toList

    sm.LoadOneItemIP(list_baidutop_people, "baitop_peo")
    //sm.LoadOneItemIP(list_baidutop_movie, "baitop_movie")
    //sm.LoadOneItemIP(list_baidutop_tv, "baitop_tv")
    //sm.LoadOneItemIP(list_baidutop_variety, "baitop_vari")
    //sm.LoadOneItemIP(list_baidutop_music, "baitop_music")
    //sm.LoadOneItemIP(list_baidutop_cartoon, "baitop_cartoon")

    sm.LoadMultiItemIP(list_headline_baidu_today_hotsearch, "headline_baidu", 2)
    sm.LoadMultiItemIP(list_headline_ifeng, "headline_ifeng", 2)
    sm.LoadMultiItemIP(list_headline_sina, "headline_sina", 2)
    sm.LoadMultiItemIP(list_finance_ifeng, "finance_ifeng", 2)
    sm.LoadMultiItemIP(list_sport_sina, "sport_sinae", 2)
    val broadcastSm = sc.broadcast(sm)

    val result=documents.map{e=>
      val stringMatch = broadcastSm.value
      val res : List[Integer]  = sm.Match(e.media_doc_info.name).toList
      val hash_map = new scala.collection.mutable.HashMap[Int, Int]
      for (s<-res) {
        val entry = stringMatch.entries_.get(s)
        for (ip<-entry.related_IPLIST){
          hash_map.put(ip,1+hash_map.getOrElse(ip,0))
        }
      }


      for (i<-hash_map) {
        val ip = stringMatch.IPList_.get(i._1)
        val total_entry :Double = ip.total_entry.size()
        val match_entry: Double = i._2

        if (match_entry / total_entry > 0.6) {
          val item = new ItemFeature()
          item.setName(ip.label)
          item.setWeight(ip.weight.toShort)
          item.setType(FeatureType.HOT_WORD)
          e.feature_list.add(item)

          println(e.media_doc_info.name)
          println(item.name +  ":"  + item.weight + ", " + ip.name + ", " + i._2 + ", " + total_entry)
        }
      }
      e
    }
//    documents.foreach(println(_))
    result
  }


  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    val sparkConf = new SparkConf()
    val sc = new SparkContext(masterUrl, "MatchTest", sparkConf)
    val rddComposite=sc.
      textFile("D:\\Temp\\hdfs_data_input")
      .map(e=>e.split("\t")).map(e=>(e(0),e(1)))
      .map(e=>DocProcess.CompositeDocSerialize.DeSerialize(e._2, null))

    val result = ProcessByMatchHotTag(rddComposite)
    println(result.count());

  }
}
