package Component.nlp

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD

import collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
/**
  * Created by lujing1 on 2016/12/20.
  */
object Word2Vector {
  def getEntityRelation(rdd:RDD[Text]):Array[(String,String,Double)]={
    val model=getWordVector(rdd)
    val result=getEntityRelation(model,rdd)
    result
  }
  def getEntityRelation(model:Word2VecModel,rdd:RDD[Text]):Array[(String,String,Double)]={
    val entityWords=rdd.flatMap(_.wordTextRank.keySet().map(e=>(e,1)))
      .reduceByKey(_+_).map(e=>e._1).collect().toSet
    val model=getWordVector(rdd)
    val result=ArrayBuffer[(String,String,Double)]()
    for(word<-entityWords){
      result.appendAll(model.findSynonyms(word,100).filter(e=>entityWords.contains(e._1)).map(e=>(word,e._1,e._2)))
    }
    result.toArray
  }
  def getWordVector(rdd:RDD[Text]):Word2VecModel={
    val input = rdd.flatMap(_.spliteSentences).map(_.toSeq)
    input.foreach(println(_))
    val word2vec = new Word2Vec()
    word2vec.setMinCount(1)
    val model = word2vec.fit(input)
    model
  }

}
