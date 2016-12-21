package ldacore

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LocalLDAModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Success

/**
  * Created by lujing1 on 2016/10/19.
  */
object CalLDA {
  var minWordNum=0
  def indexWithWord[T:ClassTag](rdd: RDD[(T, List[(String, Double)])]): Map[String, Int] = {
    val wordIndex = rdd.flatMap(_._2.map(_._1)).map((_, 1)).reduceByKey(_ + _)
      .filter(_._2 >minWordNum)
      .map(_._1).collect().zipWithIndex.toMap
    //wordIndex.toArray.sortBy(_._2).reverse.foreach(e=>println(e._2+"\t"+e._1))
    wordIndex
  }

  def getCorpus[T:ClassTag](rdd: RDD[(T, List[(Int, Double)])], size: Int): RDD[(T, Vector)] = {
    val corpus = rdd.mapValues { line =>
      val vector = Vectors.sparse(size, line.map(e => (e._1, e._2.toInt.toDouble)))
      vector
    }
    corpus
  }

  def lda[T:ClassTag](corpus: RDD[(T, Vector)], size: Int, topicNum: Int): (RDD[(T, Vector)], LocalLDAModel) = {
    val filterCorpus = corpus.zipWithIndex().map(e => (e._2, e._1._2))

    val ldaModel = new LDA().setK(topicNum).run(filterCorpus)

    val distributedLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]

    val localModel = distributedLDAModel.toLocal

    val result=corpus.zip(localModel.topicDistributions(filterCorpus))
      .map(e=>(e._1._1,e._2._2))
    (result, localModel)
  }

  def convertStringToId(str: String): Long = {
    val idLong = scala.util.Try(str.toLong)
    val result = idLong match {
      case Success(_) => idLong.get
      case _ => -1
    }
    result
  }

  def getNewsWordId[T:ClassTag](rdd: RDD[(T, List[(String, Double)])], wordIndex: Map[String, Int]): RDD[(T, List[(Int, Double)])] = {
    val sc = rdd.sparkContext
    val wordIndexBroadcast = sc.broadcast(wordIndex)
    val result = rdd.mapValues { list =>
      val mapWI = wordIndexBroadcast.value
      list.map(e => (mapWI.getOrElse(e._1, -1), e._2))
        .filter { e => (e._1 > 0) && (e._1 < mapWI.size)}
    }
      .filter(_._2.size > 0)
    result
  }

  def getLDAModel[T:ClassTag](sc: SparkContext, news:RDD[(T, List[(String, Double)])], numTopic: Int):(RDD[(T, Vector)],Map[String,Int],LocalLDAModel)= {

    val wordIndex = indexWithWord(news)
    val paperWordId = getNewsWordId(news, wordIndex)
    val corpus = getCorpus(paperWordId, wordIndex.size)

    val (topicDistributions, localModel) = lda(corpus, wordIndex.size, numTopic)
    corpus.unpersist()
    ////
    //    if (outputpath != null) {
    //      //localModel.save(sc, outputpath + "ldamodel")
    //      sc.parallelize(wordIndex.toList.sortBy(_._2)).saveAsTextFile(outputpath + "wordIndex")
    //      //topicDistributions.mapValues(_.toArray.mkString("\t")).map(line => line._1 + ":" + line._2).saveAsTextFile(outputpath + "IdVector")
    //
    //    }
    (topicDistributions,wordIndex,localModel)
  }



}
