package Component.DocumentProcess

import java.io.File
import java.util

import Component.Util.JNACall.CLibrary
import Component.Util.StringMatch
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import pipeline.CompositeDoc
import shared.datatypes.{FeatureType, ItemFeature}

import scala.collection.immutable.HashMap
import scala.collection.mutable

// used for java scala type exchange
import scala.collection.JavaConversions._
/**
  * Created by sunhaochuan on 2017/2/16.
  */
object ToutiaoTagMatcher {

  def FastTextPrepare(sc : SparkContext):  Broadcast[StringMatch] = {
    val input_toutiao_data = "../lib/model/toutiao_tag.txt"
    //val input_hot_data_dir = "D:\\Temp\\toutiao_tag.txt"

    StringMatch.sm.LoadFile(input_toutiao_data)
    val broadcastSm = sc.broadcast(StringMatch.sm)

    val rdd_temp = sc.parallelize("abcccd")
    rdd_temp.foreach( e => {
      broadcastSm.value.Match(e.toString)
    })

    (broadcastSm)
  }

  def IsIncluded(current: mutable.HashSet[String], tag : String) : Boolean = {
    var res = false;
    current.map(e=> {
      if (e.contains(tag) || e.equals(tag)) {
        res = true
      }
    })

    res
  }

  val categoryMapping : Map[String, Set[String]] = Map(
    "三农"->Set("2"),
    "传媒"->Set("0"),
    "体育"->Set("4","2"),
    "体育视频"->Set("4", "2"),
    "佛学"->Set("0"),
    "健康"->Set("11", "12"),
    "其他"->Set("0"),
    "其它"->Set("0"),
    "养生"->Set("11", "12"),
    "军事"->Set("9"),
    "动漫"->Set("13"),
    "历史"->Set("0"),
    "商业"->Set("2","8"),
    "国际"->Set("2"),
    "图片"->Set("0"),
    "奇葩"->Set("5"),
    "娱乐"->Set("3"),
    "婚嫁"->Set("12"),
    "宠物"->Set("5"),
    "家居"->Set("11"),
    "小说"->Set("0"),
    "彩票"->Set("0"),
    "心理"->Set("11", "12"),
    "情感"->Set("11", "12"),
    "房产"->Set("2"),
    "技术"->Set("8", "2"),
    "搞笑"->Set("5"),
    "摄影"->Set("0"),
    "收藏"->Set("0"),
    "故事"->Set("0"),
    "教育"->Set("0"),
    "数码"->Set("7", "2"),
    "文化"->Set("6"),
    "旅游"->Set("11"),
    "时尚"->Set("3"),
    "时尚视频"->Set("3"),
    "时政"->Set("1","2"),
    "星座"->Set("0"),
    "正能量"->Set("0"),
    "汽车"->Set("10"),
    "游戏"->Set("13"),
    "电影视频"->Set("3"),
    "社会"->Set("1"),
    "科学"->Set("8", "2"),
    "科技"->Set("8", "2"),
    "移民"->Set("2"),
    "美女"->Set("0"),
    "美文"->Set("0"),
    "美食"->Set("11"),
    "职场"->Set("0"),
    "育儿"->Set("11"),
    "视频"->Set("0"),
    "设计"->Set("0"),
    "财经"->Set("2"),
    "财经视频"->Set("2"),
    "辟谣"->Set("0"),
    "音乐视频"->Set("3")

  )
  def MatchPostProcess(stringMatch: StringMatch, tag: String, tag_category: String) : Boolean = {
    var res : Boolean = false;
    val si = stringMatch.sm_info.get(tag)
    for( kv<-si.category_info) {
      if (kv._2 > 0.1 && categoryMapping.getOrElse(kv._1, null) != null && categoryMapping.get(kv._1).get(tag_category) == true) {
        res = true
      }
    }
    res
  }

  def ProcessByMatchToutiaoTag(documents: RDD[(String, CompositeDoc, String)], broadcastSm : Broadcast[StringMatch]) : RDD[(String, CompositeDoc, String)] = {
    val sc = documents.sparkContext

    //val toutiao_blacklist=Set("花","她","虎", "马")
    val toutiao_blacklist = Set()


    val result=documents.map{e=>
      val stringMatch = broadcastSm.value
      val res : List[Integer]  = stringMatch.Match(e._2.media_doc_info.name).toList
      val hash_map = new scala.collection.mutable.HashMap[Int, Int]
      for (s<-res) {
        val entry = stringMatch.entries_.get(s)
        for (ip<-entry.related_IPLIST){
          hash_map.put(ip,1+hash_map.getOrElse(ip,0))
        }
      }
      e._2.setBody_np(new util.ArrayList[String] )

      // add the feature to feature list
      val dedup_set = new mutable.HashSet[String]()
      e._2.feature_list.map(item =>
      {
        if (!dedup_set.contains(item.name)) {
          dedup_set.add(item.name)
        }
      })

      for (i<-hash_map) {
        val ip = stringMatch.IPList_.get(i._1)
        val total_entry :Double = ip.total_entry.size()
        val match_entry: Double = i._2

        e._2.body_np.add(ip.name)
        if (!dedup_set.contains(ip.name) && !toutiao_blacklist.contains(ip.name) && !IsIncluded(dedup_set, ip.name) && MatchPostProcess(stringMatch, ip.name, e._2.media_doc_info.category_name)) {
          val item = new ItemFeature()
          item.setName(ip.name)
          item.setWeight(ip.weight.toShort)
          //item.setType(FeatureType.NNP)
          item.setType(FeatureType.NP)
          e._2.feature_list.add(item)

          dedup_set.add(ip.name)
        }


      }
      //val temp =  e._2.media_doc_info.id + "\t" +  e._2.media_doc_info.play_url + "\t" + e._2.body_np.toString + "\t" + e._2.feature_list.map(e=>{e.getName}) + "\n"
      //import java.io._
      //val writer = new PrintWriter(new File("D:\\Temp\\abc.txt"),"UTF-8")
      //val writer = new FileWriter("D:\\Temp\\abc.txt",true)
      //writer.write(temp)
      //writer.close()
      (e._1, e._2, e._3)
    }

    result
  }

}
