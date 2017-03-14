package Component.DocumentProcess

import java.util

import Component.Util.JNACall.CLibrary
import Component.Util.StringMatch
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
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

  def ProcessByMatchToutiaoTag(documents: RDD[(String, CompositeDoc, String)], broadcastSm : Broadcast[StringMatch]) : RDD[(String, CompositeDoc, String)] = {
    val sc = documents.sparkContext


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
      for (i<-hash_map) {
        val ip = stringMatch.IPList_.get(i._1)
        val total_entry :Double = ip.total_entry.size()
        val match_entry: Double = i._2

        val item = new ItemFeature()
        item.setName(ip.label)
        item.setWeight(ip.weight.toShort)
        //item.setType(FeatureType.NNP)
        item.setType(FeatureType.TAG)
        e._2.feature_list.add(item)

        e._2.body_np.add(ip.label)
      }
      (e._1, e._2, e._3)
    }
    result
  }
}
