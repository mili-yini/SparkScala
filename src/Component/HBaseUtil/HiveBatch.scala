package Component.HBaseUtil

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql._



/**
  * Created by sunhaochuan on 2017/3/9.
  */
object HiveBatch {

  case class test_a_b(a:String, b:String)
  def WriteToHiveKVTable(rdd : RDD[(String, String)], tableName : String) : Int = {



    val sc = rdd.sparkContext
    val hc = new HiveContext(sc)
    import hc.implicits._
    val df = rdd.map(x=>test_a_b(x._1,x._2)).toDF
    df.registerTempTable("tmp_table")
    val sql = "insert into table %s select a,b from tmp_table ".format(tableName)
    println("sql:\t" + sql)
    hc.sql(sql)

    0
  }

}
