package Component.DocumentProcess

import Component.Util.StringMatch
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
  def ProcessByMatchHotTag(documents: RDD[CompositeDoc]) : RDD[CompositeDoc] = {
    val sc = documents.sparkContext

    //val input_hot_data_dir = "D:\\Temp\\toutiao_tag.txt"
    val input_hot_data_dir = "/data/overseas_in/recommendation/galaxy/hot/"

    val sm : StringMatch = new StringMatch()

    //http://10.148.12.101:8000/wanghongqing/chenglinpeng/leview_movie/data/hot_people
    val list_baidutop_people = sc.textFile(input_hot_data_dir + "hot_people").collect().toList
    //http://10.148.12.101:8000/wanghongqing/chenglinpeng/leview_movie/data/movie_all
    val list_baidutop_movie = sc.textFile(input_hot_data_dir + "movie_all").collect().toList
    //http://10.148.12.101:8000/wanghongqing/chenglinpeng/leview_movie/data/TVplay_all
    val list_baidutop_tv = sc.textFile(input_hot_data_dir + "TVplay_all").collect().toList
    //http://10.148.12.101:8000/wanghongqing/chenglinpeng/leview_movie/data/variety_all
    val list_baidutop_variety = sc.textFile(input_hot_data_dir + "variety_all").collect().toList
    //val list_baidutop_music = sc.textFile("").collect().toList
    //http://10.148.12.101:8000/wanghongqing/chenglinpeng/leview_movie/data/cartoon_all
    val list_baidutop_cartoon = sc.textFile(input_hot_data_dir + "cartoon_all").collect().toList

    //http://10.148.12.101:8000/wanghongqing/leview_news/data/baidu_today_hotsearch.txt
    val list_headline_baidu_today_hotsearch = sc.textFile(input_hot_data_dir + "baidu_today_hotsearch.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/baidu_realtime_hot_search.txt
    val list_headline_baidu_realtime_hotsearch = sc.textFile(input_hot_data_dir + "baidu_realtime_hot_search.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/qq_headline.txt
    val list_headline_qq = sc.textFile(input_hot_data_dir + "qq_headline.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/ifeng_headline.txt
    val list_headline_ifeng = sc.textFile(input_hot_data_dir + "ifeng_headline.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/sina_headline.txt
    val list_headline_sina = sc.textFile(input_hot_data_dir + "sina_headline.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/ifeng_finance.txt
    val list_finance_ifeng = sc.textFile(input_hot_data_dir + "ifeng_finance.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/sina_finance.txt
    val list_finace_sina = sc.textFile(input_hot_data_dir + "sina_finance.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/hupu_sport.txt
    val list_sport_hupu = sc.textFile(input_hot_data_dir + "hupu_sport.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/qq_sport.txt
    val list_sport_qq = sc.textFile(input_hot_data_dir + "qq_sport.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/sina_sport.txt
    val list_sport_sina = sc.textFile(input_hot_data_dir + "sina_sport.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/ifeng_ent.txt
    val list_ent_ifeng = sc.textFile(input_hot_data_dir + "ifeng_ent.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/qq_ent.txt
    val list_ent_qq = sc.textFile(input_hot_data_dir + "qq_ent.txt").collect().toList
    //http://10.148.12.101:8000/wanghongqing/leview_news/data/sina_ent.txt
    val list_ent_sina = sc.textFile(input_hot_data_dir + "sina_ent.txt").collect().toList

    sm.LoadOneItemIP(list_baidutop_people, "baitop_peo", 10)
    sm.LoadOneItemIP(list_baidutop_movie, "baitop_movie", 10)
    sm.LoadOneItemIP(list_baidutop_tv, "baitop_tv", 10)
    sm.LoadOneItemIP(list_baidutop_variety, "baitop_vari", 10)
    //sm.LoadOneItemIP(list_baidutop_music, "baitop_music")
    sm.LoadOneItemIP(list_baidutop_cartoon, "baitop_cartoon", 10)

    sm.LoadMultiItemIP(list_headline_baidu_today_hotsearch, "hot_search", 2, 10)
    sm.LoadMultiItemIP(list_headline_baidu_realtime_hotsearch, "hot_search", 2, 30)
    sm.LoadMultiItemIP(list_headline_ifeng, "hot_headline", 2, 10000)
    sm.LoadMultiItemIP(list_headline_sina, "hot_headline", 2, 10000)
    sm.LoadMultiItemIP(list_headline_qq, "hot_headline", 2, 10000)
    sm.LoadMultiItemIP(list_finance_ifeng, "hot_finance", 2, 10000)
    sm.LoadMultiItemIP(list_finace_sina, "hot_finance", 2, 10000)
    sm.LoadMultiItemIP(list_sport_hupu, "hot_sport", 2, 10000)
    sm.LoadMultiItemIP(list_sport_qq, "hot_sport", 2, 10000)
    sm.LoadMultiItemIP(list_sport_sina, "hot_sport", 2, 10000)
    sm.LoadMultiItemIP(list_ent_ifeng, "hot_ent", 2, 10000)
    sm.LoadMultiItemIP(list_ent_qq, "hot_ent", 2, 10000)
    sm.LoadMultiItemIP(list_ent_sina, "hot_ent", 2, 10000)

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


      val hot_tag = new HashMap[String, Int]
      for (i<-hash_map) {
        val ip = stringMatch.IPList_.get(i._1)
        val total_entry :Double = ip.total_entry.size()
        val match_entry: Double = i._2

        if (match_entry / total_entry >= 0.6) {
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
