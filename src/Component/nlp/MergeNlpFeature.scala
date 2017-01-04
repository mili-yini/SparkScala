package Component.nlp

import java.lang.Double
import java.util

import ldacore.CalLDA
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import pipeline.CompositeDoc
import collection.JavaConverters._
import scala.collection.JavaConversions._
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

  def mergeLDAFeature(rdd:RDD[CompositeDoc], outputPath:String): RDD[CompositeDoc] = {
    val sc = rdd.sparkContext
    val compostic=rdd.map(e => (e.media_doc_info.id, e))
    val feature = sc.textFile(outputPath)
      .map(e => e.split("\t")).map(e => (e(0), e(1).split(" ").map(ee => ee.toDouble)))
    //rdd.foreach(e => println(e._1))
    val result=compostic.leftOuterJoin(feature).map{ e =>
      val doc = e._2._1
      val f = e._2._2
      f match {
        case Some(v)=>addLDAFeature(doc,v)
        case _=> doc.media_doc_info.setLdavec(new util.ArrayList[Double]())
      }
      doc
    }
    result
  }
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    val sparkConf = new SparkConf()
    val sc = new SparkContext(masterUrl, "SparkHBaseDBExtraction", sparkConf)
    val rddBase64=sc.
      textFile("C:\\Users\\lujing1\\Desktop\\LabelTag\\hdfs_data_input")
      .map(e=>e.split("\t")).map(e=>(e(0),e(1)))
      .map(e=>DocProcess.CompositeDocSerialize.DeSerialize(e._2, null))
    calLDAFeature(rddBase64,"D:\\a")
    val result=mergeLDAFeature(rddBase64,"D:\\a")
    result.foreach(e=>println(e.media_doc_info.getLdavec.mkString(" ")))

  }


}
