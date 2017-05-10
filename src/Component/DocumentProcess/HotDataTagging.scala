package Component.DocumentProcess

import java.util.Date

import Component.Util.StringMatch
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import pipeline.CompositeDoc
import shared.datatypes.{FeatureType, ItemFeature}

import scala.collection.immutable.HashMap

// used for java scala type exchange
import scala.collection.JavaConversions._

/**
  * Created by sunhaochuan on 2017/2/15.
  */
object HotDataTagging {

  def FastTextPrepare(sc : SparkContext):  Broadcast[StringMatch] = {
    //val input_hot_data_dir = "D:\\Temp\\toutiao_tag.txt"
    //val input_hot_data_dir = "/data/overseas_in/recommendation/galaxy/hot/"
    //val input_hot_data_dir = "/data/rec/recommendation/galaxy/doc_process/hot/"
    val input_hot_data_dir = "../data/hot/"

    val sm : StringMatch = new StringMatch()

    sm.LoadOneItemIP(input_hot_data_dir + "hot_people", "baitop_peo", 10)
    sm.LoadOneItemIP(input_hot_data_dir + "movie_all", "baitop_movie", 10)
    sm.LoadOneItemIP(input_hot_data_dir + "TVplay_all", "baitop_tv", 10)
    sm.LoadOneItemIP(input_hot_data_dir + "variety_all", "baitop_vari", 10)
    //sm.LoadOneItemIP(list_baidutop_music, "baitop_music")
    sm.LoadOneItemIP(input_hot_data_dir + "cartoon_all", "baitop_cartoon", 10)

    sm.LoadMultiItemIP(input_hot_data_dir + "baidu_today_hotsearch.txt", "hot_search", 2, 10)
    sm.LoadMultiItemIP(input_hot_data_dir + "baidu_realtime_hot_search.txt", "hot_search", 2, 30)
    sm.LoadMultiItemIP(input_hot_data_dir + "ifeng_headline.txt", "hot_headline", 2, 10000)
    sm.LoadMultiItemIP(input_hot_data_dir + "sina_headline.txt", "hot_headline", 2, 10000)
    sm.LoadMultiItemIP(input_hot_data_dir + "qq_headline.txt", "hot_headline", 2, 10000)
    sm.LoadMultiItemIP(input_hot_data_dir + "ifeng_finance.txt", "hot_finance", 2, 10000)
    sm.LoadMultiItemIP(input_hot_data_dir + "sina_finance.txt", "hot_finance", 2, 10000)
    sm.LoadMultiItemIP(input_hot_data_dir + "hupu_sport.txt", "hot_sport", 2, 10000)
    sm.LoadMultiItemIP(input_hot_data_dir + "qq_sport.txt", "hot_sport", 2, 10000)
    sm.LoadMultiItemIP(input_hot_data_dir + "sina_sport.txt", "hot_sport", 2, 10000)
    sm.LoadMultiItemIP(input_hot_data_dir + "ifeng_ent.txt", "hot_ent", 2, 10000)
    sm.LoadMultiItemIP(input_hot_data_dir + "qq_ent.txt", "hot_ent", 2, 10000)
    sm.LoadMultiItemIP(input_hot_data_dir + "sina_ent.txt", "hot_ent", 2, 10000)

    val broadcastSm = sc.broadcast(sm)
    broadcastSm
  }

  def ProcessByMatchHotTag(documents: RDD[CompositeDoc], broadcastSm : Broadcast[StringMatch]) : RDD[CompositeDoc] = {
    val sc = documents.sparkContext



    val result=documents.map { e =>
      e.media_doc_info.setHotness(0)
      val now = new Date();
      var now_timestamp: Long = now.getTime() / 1000;
      if (now_timestamp - e.media_doc_info.crawler_timestamp <= 24 * 3600) {
        val stringMatch = broadcastSm.value
        val res: List[Integer] = stringMatch.Match(e.media_doc_info.name).toList
        val hash_map = new scala.collection.mutable.HashMap[Int, Int]
        for (s <- res) {
          val entry = stringMatch.entries_.get(s)
          for (ip <- entry.related_IPLIST) {
            hash_map.put(ip, 1 + hash_map.getOrElse(ip, 0))
          }
        }


        val hot_tag = new HashMap[String, Int]
        for (i <- hash_map) {
          val ip = stringMatch.IPList_.get(i._1)
          val total_entry: Double = ip.total_entry.size()
          val match_entry: Double = i._2

          if (match_entry / total_entry >= 0.6) {
            e.media_doc_info.setHotness(e.media_doc_info.hotness + 1)
            val item = new ItemFeature()
            item.setName(ip.label)
            item.setWeight(ip.weight.toShort)
            item.setType(FeatureType.HOT_WORD)
            e.feature_list.add(item)

            println(e.media_doc_info.name)
            println(item.name + ":" + item.weight + ", " + ip.name + ", " + i._2 + ", " + total_entry)
          }
        }
      }
      e
    }

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

    //val result = ProcessByMatchHotTag(rddComposite)
    //println(result.count());

  }
}
