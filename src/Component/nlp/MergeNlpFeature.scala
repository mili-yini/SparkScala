package Component.nlp

import java.lang.Double
import java.util

import ldacore.CalLDA
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import pipeline.CompositeDoc
import shared.datatypes.{FeatureType, ItemFeature}

import collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/**
  * Created by lujing1 on 2016/12/20.
  */
object MergeNlpFeature {
  def calLDAFeature(rdd:RDD[CompositeDoc],outputPath:String): Unit ={
    //val rdd=rddBase64.map(e=>DocProcess.CompositeDocSerialize.DeSerialize(e._2, null))
    val sc=rdd.sparkContext
    val feature=rdd.map(e=>(e,e.feature_list)).filter(_._2!=null).filter(_._2.length>0)
        .mapValues(_.toList.map(ee=>(ee.name,ee.weight.toDouble)))
          .map(e=>(e._1.media_doc_info.id,e._2))
    val result=CalLDA.getLDAModel[String](sc,feature,100)._1
      .map(e=>(e._1+"\t"+e._2.toArray.mkString(" "))).cache()

    if(outputPath.length>0){
       result.saveAsTextFile(outputPath)
    }else{
      result.foreach(e=>println(e))
    }
  }
  def addLDAFeature(doc:CompositeDoc,vector:Array[scala.Double]): Unit ={
    doc.media_doc_info.setLdavec(new util.ArrayList[Double]())
    for(s<-vector){
       doc.media_doc_info.ldavec.add(s)
    }
  }

  def addFeature(doc:CompositeDoc,value:Iterable[String]): Unit = {
    for (s<-value) {
      val raw_feature = s.split("\t")
      if (raw_feature.size < 2) {
        return 1
      }
      raw_feature(0) match {
        case  "doc2vec"=> {
          val double_vec = raw_feature(1).split(" ").map(_.toDouble)
          if (double_vec.size == 100) {
              doc.media_doc_info.setDoc2vec(new util.ArrayList[Double]())
            for (s <- double_vec) {
              doc.media_doc_info.doc2vec.add(s)
            }
          }
        }
      }
    }

    return 0
  }

  def mergeLDAFeature(rdd:RDD[CompositeDoc], outputPath:String): RDD[CompositeDoc] = {
    val sc = rdd.sparkContext
    val compostic=rdd.map(e => (e.media_doc_info.id, e))
    val feature = sc.textFile(outputPath)
          .map(e => e.split("\t")).filter(_.length == 3).map(e => (e(0), e(1) + "\t" + e(2)))
             .groupByKey()
    //feature.foreach(e=> println(e._2))
    //println(feature.count())
      //.map(e => e.split("\t")).map(e => (e(0), e(1).split(" ").map(ee => ee.toDouble)))
    //rdd.foreach(e => println(e._1))
    val result=compostic.leftOuterJoin(feature).map{ e =>
      val doc = e._2._1
      val f = e._2._2
      f match {
        case Some(v)=>addFeature(doc,v)
        case _=>
      }
      doc
    }
    result
  }
  val toutiao_blacklist=Set("花","她","虎", "马", "桃")
  def mergeFastTexFeature(rdd: RDD[CompositeDoc], outputPath: String): RDD[CompositeDoc] = {
    val sc = rdd.sparkContext
    val compositeDoc = rdd.map(e=>(e.media_doc_info.id, e))

    val toutiao_feature = sc.textFile(outputPath).map(e=>e.split("\t")).filter(_.length==3).map(e=> (e(0), e(1) + "\t" +  e(2)))
    //dedup the duplicate the toutiao tag and return the last one
    val dedup_toutiaofeature = toutiao_feature.reduceByKey((x,y) => y)

    val join_res = compositeDoc.leftOuterJoin(dedup_toutiaofeature).map{ e=>
      val doc = e._2._1
      val f = e._2._2

      doc.body_nnp = new util.ArrayList[String]
      if (f.isEmpty) {
        val temp_feature_list = doc.feature_list.map(e => {
          if(e.getType() == FeatureType.NP && toutiao_blacklist.contains(e.getName()) == false) {
            e.setType(FeatureType.TAG)
          }
          e
        }
        ).filter(e=>(e.getType() != FeatureType.NP))
        doc.feature_list = temp_feature_list
        doc.body_nnp.add("NoJoin")
      } else {

        val dedup_table = new mutable.HashMap[String, ItemFeature]()
        doc.feature_list.map(item => {
          if (item.getType != FeatureType.NP) {
            dedup_table.put(item.name, item)
          }

        })

        for (s<-f) {
          val tags_str = s.split("\t")
          if (tags_str.size == 2) {
            val tags = tags_str(1).split(";")
            for (tag<-tags) {
              if (!dedup_table.contains(tag)) {
                val item = new ItemFeature()
                item.setName(tag)
                item.setWeight(1)
                item.setType(FeatureType.TAG)
                //doc.feature_list.add(item)
                dedup_table.put(tag, item)
                doc.body_nnp.add(tag)
              }
            }
            val categorys = tags_str(0).split(";")
            for (category<-categorys) {
              if (!dedup_table.contains(category)) {
                val item = new ItemFeature()
                item.setName(category)
                item.setWeight(1)
                item.setType(FeatureType.TAG)
                //doc.feature_list.add(item)
                dedup_table.put(category, item)
                doc.body_nnp.add(category)
              }
            }
          }
        }

        doc.feature_list.clear()
        for (kv<-dedup_table) {
          if (toutiao_blacklist.contains(kv._2.getName()) == false) {
            doc.feature_list.add(kv._2)
          }
        }
      }

      doc.media_doc_info.feature_list.clear()
      doc.feature_list.map(item => doc.media_doc_info.feature_list.put(item.name, item))

      doc
    }

    join_res
  }
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    val sparkConf = new SparkConf()
    val sc = new SparkContext(masterUrl, "SparkHBaseDBExtraction", sparkConf)
    val rddComposite=sc.
      textFile("D:\\Temp\\hdfs_data_input")
      .map(e=>e.split("\t")).map(e=>(e(0),e(1)))
      .map(e=>DocProcess.CompositeDocSerialize.DeSerialize(e._2, null))

    val result = MergeNlpFeature.mergeLDAFeature(rddComposite, "D:\\Temp\\doc2vec")

    result.foreach(e=>println(e.media_doc_info.doc2vec.mkString(" ")))

  }


}
