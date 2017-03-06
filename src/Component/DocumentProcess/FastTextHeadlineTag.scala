package Component.DocumentProcess

import java.util

import Component.Util.JNACall.CLibrary
import Component.Util.{StringMatch, ZhCnWordProcess}
import org.apache.spark.rdd.RDD
import pipeline.CompositeDoc
import shared.datatypes.{FeatureType, ItemFeature}

import scala.collection.JavaConversions._
/**
  * Created by sunhaochuan on 2017/3/3.
  */
object FastTextHeadlineTag {



  def ProcessByMatchToutiaoTag(documents: RDD[(String, CompositeDoc, String)]) : RDD[(String, CompositeDoc, String)] = {
    val sc = documents.sparkContext
    CLibrary.INSTANCE.LoadModel("../lib/model/ft_category.bin", 0)
    CLibrary.INSTANCE.LoadModel("../lib/model/ft_tag.bin", 1)
    val broadcastInstance = sc.broadcast(CLibrary.INSTANCE)


    val input_hot_data_dir = "../lib/model/toutiao_tag.txt"
    //val input_hot_data_dir = "D:\\Temp\\toutiao_tag.txt"
    val sm : StringMatch = new StringMatch()
    val list_toutiao_tag = sc.textFile(input_hot_data_dir).collect().toList
    sm.LoadTagIP(list_toutiao_tag)
    val broadcastSm = sc.broadcast(sm)


    val result=documents.map{e=>

      // prepare the input
      val title = e._2.media_doc_info.name;
      var  body =  ZhCnWordProcess.JoinBody(e._2.main_text_list);
      if (body == null) {
        body = e._2.description
      }
      var raw_input = title;
      if (body != null) {
        raw_input = title + " " + body;
      }

      // fasttext predict
      val instance = broadcastInstance.value
      val category_raw_output = instance.Predict(raw_input, 5, 0)
      val tag_raw_output = instance.Predict(raw_input, 100, 1)

      // stringmatch match
      val stringMatch = broadcastSm.value
      val res : List[Integer]  = sm.Match(title).toList
      val hash_map : scala.collection.mutable.Map[String, Integer] = new scala.collection.mutable.HashMap[String, Integer]
      for (s<-res) {
        val entry = stringMatch.entries_.get(s)
        for (ip<-entry.related_IPLIST){
          val ip_item = stringMatch.IPList_.get(ip)
          val temp : Integer = hash_map.getOrElse(ip_item.label,0)
          hash_map.put(ip_item.label,1 + temp)
        }
      }

      // the category result
      val res_cate = ZhCnWordProcess.GetCategory(category_raw_output)
      // the tag result
      val java_map: java.util.Map[String, Integer] = hash_map
      val res_tag = ZhCnWordProcess.GetTag(tag_raw_output, java_map)


      // add the result to compostie doc
      e._2.setBody_np(new util.ArrayList[String] )
      for (i<-res_cate) {

        val item = new ItemFeature()
        item.setName(i)
        item.setWeight(1)
        //item.setType(FeatureType.NNP)
        item.setType(FeatureType.TAG)
        e._2.feature_list.add(item)

        e._2.body_np.add(i)
      }

      for (i<-res_tag) {

        val item = new ItemFeature()
        item.setName(i)
        item.setWeight(1)
        //item.setType(FeatureType.NNP)
        item.setType(FeatureType.TAG)
        e._2.feature_list.add(item)

        e._2.body_np.add(i)
      }

      (e._1, e._2, e._3)
    }
    result
  }

}
