package Streaming
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD


import org.bson.BSONObject
import com.mongodb.hadoop.{
MongoInputFormat, MongoOutputFormat,
BSONFileInputFormat, BSONFileOutputFormat}
import com.mongodb.hadoop.io.MongoUpdateWritable

/**
  * Created by sunhaochuan on 2016/12/27.
  */
object MongoDBReader {
  def main(args: Array[String]): Unit = {
    val mongoConfig = new Configuration()

    mongoConfig.set("mongo.input.uri",
      "mongodb://10.154.156.118:27017/galaxy.content_access")

    val sparkConf = new SparkConf()
    val sc = new SparkContext("local", "SparkExample", sparkConf)

    val documents = sc.newAPIHadoopRDD(
      mongoConfig,                // Configuration
      classOf[MongoInputFormat],  // InputFormat
      classOf[Object],            // Key type
      classOf[BSONObject])// Value type
    documents.foreach(e=> {
      println(e._1 )
      } )
  }

}
