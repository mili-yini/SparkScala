package Component.DocumentProcess

import org.apache.spark.rdd.RDD
import pipeline.CompositeDoc

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * Created by sunhaochuan on 2017/1/3.
  */
object GetDoc2VecInput {
  def Process(documents : RDD[CompositeDoc]) : RDD[(String, String)] = {
    val processedRDD = documents.map(line => {
      (line.media_doc_info.id, line.title_words.mkString(" , ")+ " , " + line.body_words.mkString(" , "))
    })
    processedRDD
  }
}
