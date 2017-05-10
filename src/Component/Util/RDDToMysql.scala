package Component.Util
import java.nio.file.FileSystem
import java.sql.{Connection, Date, DriverManager, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, GregorianCalendar}
import javax.naming.Context

import DocProcess.CompositeDocSerialize
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import pipeline.CompositeDoc
import shared.datatypes.{FeatureType, ItemFeature}

import scala.collection.JavaConversions._

/**
  * Created by sunhaochuan on 2017/4/11.
  */
object RDDToMysql {

  def main(args:Array[String]) : Unit = {
    var masterUrl = "local[2]"
    var path = "file:///home/overseas_in/zh-cn/galaxy_aggregate_pipeline/latest/input/hdfs_data_input"
    //path = "D:/Temp/1.txt"
    if (args.length > 0) {
      path = args(0)
    }

    val sparkConf = new SparkConf() //.setMaster(masterUrl).setAppName("ProdBatch")
    val sc = new SparkContext(masterUrl, "WriteToMysql", sparkConf)
    val inputRDD = sc.textFile(path,2)
    var date  = new java.util.Date();
    val now_timestamp = date.getTime / 1000

    inputRDD.map(line => {line.split("\t")}).filter(_.length == 2).map(e => {
      val context: Context = null;
      CompositeDocSerialize.DeSerialize(e.apply(1), null)
    }).filter(now_timestamp - _.media_doc_info.crawler_timestamp < 3600 * 6).foreachPartition(WriteToMysql)
    //}).foreachPartition(WriteToMysql)
  }

  def WriteToMysql(iterator: Iterator[(CompositeDoc)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "REPLACE INTO recommend (category_id, info_id, source_id, status, tags, cms_tags, fasttext_tags, match_tags, title, url, crawl_time, modify_time, type_id) " +
      "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://10.154.252.64:3306/galaxy-cms", "sarrs", "SDF123sdfa11!@#$")
      conn.setAutoCommit(false);
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        var tags :String = ""
        for (item :ItemFeature  <- data.feature_list ){
          if (item.getType() == FeatureType.TAG) {
            tags = tags + item.name + ";"
          }
        }

        var cms_tag = ""
        if (data.title_ner != null) {
          cms_tag = data.title_ner.toString
        }
        var toutiaotag_match = data.title_nnp.toString
        var fasttext_tag = data.body_nnp.toString
        ps.setString(1, data.media_doc_info.category_name)
        ps.setString(2, data.media_doc_info.id)
        ps.setString(3, data.media_doc_info.source)
        ps.setInt(4, data.media_doc_info.risk_level)
        ps.setString(5, tags)
        ps.setString(6, cms_tag)
        ps.setString(7, fasttext_tag)
        ps.setString(8, toutiaotag_match)
        ps.setString(9, data.media_doc_info.name)
        ps.setString(10, data.media_doc_info.play_url)
        //ps.setDate(11, new java.sql.Date(data.media_doc_info.crawler_timestamp * 1000))
        ps.setTimestamp(11, new java.sql.Timestamp(data.media_doc_info.crawler_timestamp * 1000));
        //ps.setDate(12, new java.sql.Date(data.media_doc_info.update_timestamp * 1000))
        ps.setTimestamp(12, new Timestamp(data.media_doc_info.update_timestamp * 1000))
        ps.setInt(13, data.media_doc_info.content_type)
        ps.executeUpdate()
        //ps.addBatch()
      }
      )
      //ps.executeBatch();
      conn.commit();
    } catch {
      case e: Exception => {
        println("Mysql Exception")
        e.printStackTrace()
      }
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
}
