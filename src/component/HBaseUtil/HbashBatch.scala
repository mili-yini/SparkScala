package Component.HBaseUtil

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.util.Bytes.toBytes
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext

/**
  * Created by sunhaochuan on 2016/12/28.
  * Reference http://blog.csdn.net/liyongke89/article/details/51991132
  *           https://my.oschina.net/dongtianxi/blog/738264
  */
object HbashBatch {

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def BatchReadHBaseToRDD (tableName: String, family: String, column: String, sc: SparkContext) : RDD[(String, String)] = {

    val myConf = HBaseConfiguration.create ()
    myConf.set("hbase.zookeeper.property.clientPort", "31818");
    myConf.set("hbase.rootdir", "hdfs://in-cluster/hbase");
    myConf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver")
    myConf.set("hbase.client.keyvalue.maxsize","524288000");//最大500m
    myConf.set ("hbase.defaults.for.version.skip", "true")

    myConf.set(TableInputFormat.INPUT_TABLE, "MissionItem")

    val scan = new Scan()
    myConf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val readRDD = sc.newAPIHadoopRDD(myConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    readRDD.map( x => x._2 )
      .map( result => ( result.getRow,  result.getValue( family.getBytes(),column.getBytes() )) )
      .map( row => ( new String(row._1), new String(row._2) ) )
  }

  /*
   the function to write the RDD to Hbase
  * */

  def BatchWriteToHBase(rdd: RDD[(String,  String)], tableName: String, family: String, column: String
                       ) : Int = {

    rdd.foreachPartition {
      x => {
      val myConf = HBaseConfiguration.create ()
        myConf.set("hbase.zookeeper.property.clientPort", "31818");
        myConf.set("hbase.rootdir", "hdfs://in-cluster/hbase");
        myConf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver")
        myConf.set("hbase.client.keyvalue.maxsize","524288000");//最大500m
        myConf.set ("hbase.defaults.for.version.skip", "true")
      val myTable = new HTable (myConf, TableName.valueOf (tableName) )
      myTable.setAutoFlush (false, false) //关键点1
      myTable.setWriteBufferSize (3 * 1024 * 1024) //关键点2
      x.foreach {y => {
        //println (y (0) + ":::" + y (1) )
        val p = {
          new Put(Bytes.toBytes(y._1))
        }
        p.add (family.getBytes, column.getBytes, y._2.getBytes() )
        myTable.put (p)
        }
      }
      myTable.flushCommits () //关键点3
    }
  }
    0
  }

}
