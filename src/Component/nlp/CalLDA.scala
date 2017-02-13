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
  //对语料中的词进行编码
  def indexWithWord[T:ClassTag](rdd: RDD[(T, List[(String, Double)])]): Map[String, Int] = {
    val wordIndex = rdd.flatMap(_._2.map(_._1)).map((_, 1)).reduceByKey(_ + _)
      .filter(_._2 >minWordNum) //过滤掉出现太少的词
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

  // 构建LDA模型
  def lda[T:ClassTag](corpus: RDD[(T, Vector)], size: Int, topicNum: Int): (RDD[(T, Vector)], LocalLDAModel) = {
    //对各种字符串进行编码
    val filterCorpus = corpus.zipWithIndex().map(e => (e._2, e._1._2))

    val ldaModel = new LDA().setK(topicNum).run(filterCorpus)

    val distributedLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]

    // 将分布式的模型转化为本地的模型
    val localModel = distributedLDAModel.toLocal

    //zip 进行数组合并
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

  // T 是泛型，是文章的ID, 后面的就是这篇文章，word, weight 的一个list
  // 输出是一个三元组，第一个是文章的topic分布，第二个是词的字典编码，第三个是LDA模型，后两个是用来做增量预测的
  def getLDAModel[T:ClassTag](sc: SparkContext, news:RDD[(T, List[(String, Double)])], numTopic: Int):(RDD[(T, Vector)],Map[String,Int],LocalLDAModel)= {

    // 对此进行编码
    val wordIndex = indexWithWord(news)
    // 把每篇文章进行转码根据字典
    val paperWordId = getNewsWordId(news, wordIndex)
    // 把编码之后的文章，转化为向量
    val corpus = getCorpus(paperWordId, wordIndex.size)
    corpus.cache()
    //计算LDA模型
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
