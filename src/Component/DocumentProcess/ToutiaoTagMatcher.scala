package Component.DocumentProcess

import java.util

import Component.Util.StringMatch
import org.apache.spark.rdd.RDD
import pipeline.CompositeDoc
import shared.datatypes.{FeatureType, ItemFeature}

import scala.collection.immutable.HashMap

// used for java scala type exchange
import scala.collection.JavaConversions._
/**
  * Created by sunhaochuan on 2017/2/16.
  */
object ToutiaoTagMatcher {
  def ProcessByMatchToutiaoTag(documents: RDD[(String, CompositeDoc, String)]) : RDD[(String, CompositeDoc, String)] = {
    val sc = documents.sparkContext

    val input_hot_data_dir = "/data/overseas_in/recommendation/galaxy/toutiao_tag/*"
    //val input_hot_data_dir = "D:\\Temp\\toutiao_tag.txt"
    val sm : StringMatch = new StringMatch()
    val list_toutiao_tag = sc.textFile(input_hot_data_dir).collect().toList
    sm.LoadTagIP(list_toutiao_tag)
    val broadcastSm = sc.broadcast(sm)

    val result=documents.map{e=>
      val stringMatch = broadcastSm.value
      val res : List[Integer]  = sm.Match(e._2.media_doc_info.name).toList
      val hash_map = new scala.collection.mutable.HashMap[Int, Int]
      for (s<-res) {
        val entry = stringMatch.entries_.get(s)
        for (ip<-entry.related_IPLIST){
          hash_map.put(ip,1+hash_map.getOrElse(ip,0))
        }
      }
      e._2.setBody_np(new util.ArrayList[String] )
      for (i<-hash_map) {
        val ip = stringMatch.IPList_.get(i._1)
        val total_entry :Double = ip.total_entry.size()
        val match_entry: Double = i._2

        val item = new ItemFeature()
        item.setName(ip.label)
        item.setWeight(ip.weight.toShort)
        item.setType(FeatureType.NNP)
        e._2.feature_list.add(item)

        e._2.body_np.add(ip.label)
      }
      (e._1, e._2, e._3)
    }
    result
  }
}
