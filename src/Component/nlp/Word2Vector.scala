package Component.nlp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import pipeline.CompositeDoc

import collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
/**
  * Created by lujing1 on 2016/12/20.
  */
object Word2Vector {
  def getEntityRelation(rdd:RDD[CompositeDoc],outputPath:String):Unit={
    // 计算word2vec模型
    val model=getWordVector(rdd)
    // computer the similarity for each word
    val result=getEntityRelation(model,rdd)
    val sc=rdd.sparkContext
    // save or output the result
    if(outputPath.length>0){
       sc.parallelize(result).saveAsObjectFile(outputPath)
    }else{
      sc.parallelize(result).foreach(println(_))
    }
  }
  //
  def getEntityRelation(model:Word2VecModel,rdd:RDD[CompositeDoc]):Array[(String,String,Double)]={
    val entityWords=rdd.flatMap(_.feature_list.map(e=>(e.name,1)))
      .reduceByKey(_+_).map(e=>e._1).collect().toSet
//    val model=getWordVector(rdd )
    val result=ArrayBuffer[(String,String,Double)]()
    // find the nearest entity word for every word
    for(word<-entityWords){
      result.appendAll(model.findSynonyms(word,100).filter(e=>entityWords.contains(e._1)).map(e=>(word,e._1,e._2)))
    }
    result.toArray
  }
  // used to build  the word2vec model
  def getWordVector(rdd:RDD[CompositeDoc]):Word2VecModel={
    val input = rdd.flatMap(_.body_words).map(_.split(" ").toSeq)
    val word2vec = new Word2Vec()
    word2vec.setMinCount(1)
    val model = word2vec.fit(input)
    model
  }

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    val sparkConf = new SparkConf()
    val sc = new SparkContext(masterUrl, "SparkHBaseDBExtraction", sparkConf)
    val rddBase64=sc.textFile("C:\\Users\\lujing1\\Desktop\\LabelTag\\hdfs_data_input")
      .map(e=>e.split("\t")).map(e=>(e(0),e(1)))
      .map(e=>DocProcess.CompositeDocSerialize.DeSerialize(e._2, null))
    getEntityRelation(rddBase64,"")
  }

}
