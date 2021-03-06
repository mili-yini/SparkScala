package Component.DocumentProcess

import scala.collection.JavaConversions._
import java.text.SimpleDateFormat
import java.util.Date
import javax.naming.Context

import Component.HBaseUtil.HbashBatch
import Component.Util.JNACall.CLibrary
import Component.Util.StringMatch
import Component.nlp.Text
import CompositeDocProcess.DocumentAdapter
import org.ansj.library.UserDefineLibrary
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import pipeline.CompositeDoc
import shared.datatypes.{FeatureType, ItemFeature}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by sunhaochuan on 2016/12/28.
  */
object DocumentProcess {

  // user dict prepare
  def UserDictPrepare(sc : SparkContext):Void = {
    /*
    collect将在分布式环境下的RDD转化为本地变量
     */
    //val wordListRDD =sc.textFile("/data/overseas_in/recommendation/galaxy/user_dic").collect()
    //val wordListRDD = documents.sparkContext.textFile("D:\\Temp\\dict.txt").collect()
    /*
    wordListRDD 是一个本地变量，这些内容都是在Driver上执行的，而在RDD操作中的隐似函数是在分布式的环境下执行的。通过广播操作可以将Driver上的变量广播到各个服务器之上
    可以通过RDD在任何地方获取SparkContext
     */
    //val broWordList = sc.broadcast(wordListRDD)
    //broWordList
    null
  }


  // define the different interface to streaming and batch
  def ProcessStream( documents : RDD[(String, String)], broadcastInstance : Broadcast[CLibrary], broadcastSm : Broadcast[StringMatch]) : RDD[(String, String, String)] = {
    Process(documents, broadcastInstance, broadcastSm)
  }
  def ProcessBatch( documents : RDD[(Object, BSONObject)], broadcastInstance : Broadcast[CLibrary], broadcastSm : Broadcast[StringMatch]) : RDD[(String, String, String)] = {
    Process(documents.map(document => {(document._1.toString(), document._2.toString())}), broadcastInstance, broadcastSm)
  }
  def Process(documents : RDD[(String, String)], broadcastInstance : Broadcast[CLibrary], broadcastSm : Broadcast[StringMatch]) : RDD[(String, String, String)] = {

    /*
    mapPartitions 是操作RDD每一个partition
     */

    val InputSize = documents.count();
    val processedRDD1 = documents.mapPartitions {
      valueIterator => {
        /*
        这里的操作可以看做是在分布式环境下每一个独立模块的初始化
         */
        //添加词表
        /*for (word <- broWordList.value) {
          UserDefineLibrary.insertWord(word, "userDefine", 1000)
        }*/

        val result = valueIterator.map {
          line => {
            val doc: CompositeDoc = DocumentAdapter.FromJsonStringToCompositeDoc(line._2);
            //
            var serialized_string: String = null;
            var id: String = null;
            var date_prefix: String = null;
            if (doc != null) {
              //add by lujing
              val text = new Text(doc.media_doc_info.name, doc.description)
              text.addComopsticDoc(doc)

              id = doc.media_doc_info.id
              val dateFormat = new SimpleDateFormat("yyMMdd")
              val crawler_time = new Date(doc.media_doc_info.crawler_timestamp)
              date_prefix = dateFormat.format(doc.media_doc_info.crawler_timestamp * 1000)

            } else {
              System.err.println("Failed to parse :" + line._2)
            }
            (id, doc, date_prefix)
          }
        }
        result
      }
    }.filter(x => {
      x._1 != null && x._2 != null
    })

    val processedSize = processedRDD1.count()


    val processedRDD2 = ToutiaoTagMatcher.ProcessByMatchToutiaoTag(processedRDD1, broadcastSm).map(e => {
      // add the feature list to media doc info
      e._2.feature_list.map(item => e._2.media_doc_info.feature_list.put(item.name, item))
      val context: Context = null;
      val serialized_string = DocProcess.CompositeDocSerialize.Serialize(e._2, context)
      (e._1, serialized_string, e._3)
    })
    /*val processedRDD2 = FastTextHeadlineTag.ProcessByMatchToutiaoTag(processedRDD1, broadcastInstance, broadcastSm).map(e => {
    //val processedRDD2 = processedRDD1.map(e => {
      // add the feature list to media doc info
      val dedup_set = new mutable.HashSet[String]()
      e._2.feature_list.map(item =>
        {
          if (!dedup_set.contains(item.name)) {
            e._2.media_doc_info.feature_list.put(item.name, item)
            dedup_set.add(item.name)
          }
        })
      val context: Context = null;
      val serialized_string = DocProcess.CompositeDocSerialize.Serialize(e._2, context)
      (e._1, serialized_string, e._3)
    })*/

    // in the scala code, although you are using the java struct, you just should keep the scala code format
    val date  = new java.util.Date();
    val HbaaseSize = processedRDD2.count()

    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd  HH:mm:ss")
    val date_time_string = simpleDateFormat.format(date)
    if (InputSize != 0) {
      System.out.format("%s\tProcess info in PIPELINE1: %d, %d, %d\n", date_time_string, long2Long(InputSize), long2Long(processedSize), long2Long(HbaaseSize));
    }
    if (InputSize != processedSize || processedSize != HbaaseSize) {
      System.out.format("%s\tProcess Error in the PIPELINE1: %d, %d, %d\n", date_time_string, long2Long(InputSize), long2Long(processedSize), long2Long(HbaaseSize));
    }
    processedRDD2
  }


}
