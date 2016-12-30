package Component.nlp

import ldacore.CalLDA
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import pipeline.CompositeDoc

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
/**
  * Created by lujing1 on 2016/12/20.
  */
object MergeNlpFeature {
  def calLDAFeature(rddBase64:RDD[(String,String)],outputPath:String): Unit ={
    val rdd=rddBase64.map(e=>DocProcess.CompositeDocSerialize.DeSerialize(e._2, null))
    val sc=rdd.sparkContext
    val feature=rdd.map(e=>(e,e.feature_list)).filter(_._2!=null).filter(_._2.length>0)
        .mapValues(_.toList.map(ee=>(ee.name,ee.weight.toDouble)))
    val result=CalLDA.getLDAModel[CompositeDoc](sc,feature,100)._1
    if(outputPath.length>0){
       result.saveAsObjectFile(outputPath)
    }else{
      result.foreach(e=>println(e._2))
    }
  }
  def addLDAFeature(doc:CompositeDoc,vector:org.apache.spark.mllib.linalg.Vector): Unit ={

    val list = new java.util.ArrayList()
    for(s<-vector){
      list.add(s)
    }
    doc.media_doc_info.setLdavec(list.toList)
  }

  def mergeLDAFeature(rddBase64:RDD[(String,String)], outputPath:String): Unit ={
    val sc=rddBase64.sparkContext
    val composticDoc=rddBase64
      .map(e=>DocProcess.CompositeDocSerialize.DeSerialize(e._2, null)).map((_,1))
    val feature=sc.objectFile[(CompositeDoc,org.apache.spark.mllib.linalg.Vector)](outputPath)
    composticDoc.leftOuterJoin(feature).map{e=>
    val doc=e._1
    val f=e._2._2
    f match {
      case Some(vector)=>addLDAFeature(doc,vector)
      case _=>
    }

  }
  }

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    val sparkConf = new SparkConf()
    val sc = new SparkContext(masterUrl, "SparkHBaseDBExtraction", sparkConf)
    val rddBase64=sc.textFile("C:\\Users\\lujing1\\Desktop\\LabelTag\\hdfs_data_input")
      .map(e=>e.split("\t")).map(e=>(e(0),e(1)))
    calLDAFeature(rddBase64,"")
  }


}
