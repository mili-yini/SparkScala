package Component.Util
import java.nio.file.FileSystem
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Calendar, GregorianCalendar}
import javax.naming.Context

import DocProcess.CompositeDocSerialize
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import pipeline.CompositeDoc
import shared.datatypes.{FeatureType, ItemFeature}
import scala.collection.JavaConversions._

/**
  * Created by sunhaochuan on 2017/4/11.
  */
object RDDToMysql {

  def main(args:Array[String]) : Unit = {
    var masterUrl = "local[2]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    val sparkConf = new SparkConf() //.setMaster(masterUrl).setAppName("ProdBatch")
    val sc = new SparkContext(masterUrl, "WriteToMysql", sparkConf)

    val path = "file:///usr/local/spark/spark-1.6.0-bin-hadoop2.6/README.md"  //local file
    val inputRDD = sc.textFile(path,2)

    inputRDD.map(line => {line.split("\t")}).filter(_.length == 2).map(e => {
      val context: Context = null;
      CompositeDocSerialize.DeSerialize(e.apply(1), null)
    }).foreachPartition(WriteToMysql)
  }

  def WriteToMysql(iterator: Iterator[(CompositeDoc)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into recommend (category_id, info_id, source_id, status, tags, cms_tags, fasttext_tags, match_tags, title, url, crawl_time, modify_time) values (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://10.110.108.50:4009/spark", "galaxy_cms_w", "NjI2OuteewqGTI4NDU1")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        var tags :String = ""
        for (item :ItemFeature  <- data.feature_list ){
          tags = tags + item.name + ";"
        }
        var cms_tag = data.title_ner.toString
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
        ps.setLong(11, data.media_doc_info.crawler_timestamp)
        ps.setLong(11, data.media_doc_info.update_timestamp)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => println("Mysql Exception")
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
