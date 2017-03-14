package Component.HBaseUtil

/**
 * Created by root on 2016/9/30.
 */

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, ServerName, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Logging

import scala.collection.mutable

object HbaseTool extends Logging with Serializable{

  val table = new mutable.HashMap[String,HTable]()
  var conf: Configuration = HBaseConfiguration.create()
//  conf.set("hbase.zookeeper.quorum", "10.121.145.144")
  //  conf.set("hbase.zookeeper.property.clientPort", "31818")
  conf.set("hbase.zookeeper.property.clientPort", "31818");
  conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");
  conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver")
  conf.set("hbase.client.keyvalue.maxsize","524288000");//最大500m
//  conf.addResource(new Path("/etc/hbase/conf.cloudera.hbase/hbase-site.xml"))
//  conf.addResource(new Path("/etc/hbase/conf.cloudera.hbase/core-site.xml"))
  def setConf(c:Configuration)={
    conf = c
  }

  def getTable(tableName:String):HTable={

    table.getOrElse(tableName,{
      println("----new connection ----")
      val tbl = new HTable(conf, tableName)
      table(tableName)= tbl
      tbl
    })
  }

  def getValue(tableName:String,rowKey:String,family:String,qualifiers:Array[String]):Array[(String,String)]={
    var result:AnyRef = null
    val table_t =getTable(tableName)
    val row1 =  new Get(Bytes.toBytes(rowKey))
    val HBaseRow = table_t.get(row1)
    if(HBaseRow != null && !HBaseRow.isEmpty){
      result = qualifiers.map(c=>{
        (tableName+"."+c, Bytes.toString(HBaseRow.getValue(Bytes.toBytes(family), Bytes.toBytes(c))))
      })
    }
    else{
      result=qualifiers.map(c=>{
        (tableName+"."+c,"null")  })
    }
    result.asInstanceOf[Array[(String,String)]]
  }

  def putValue(tableName:String,rowKey:String, family:String,qualifierValue:Array[(String,String)]) {
    val table =getTable(tableName)
    val new_row  = new Put(Bytes.toBytes(rowKey))
    qualifierValue.map(x=>{
      var column: String = x._1
      val value: String = x._2
      new_row.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
      val strdate=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
      println("时间"+strdate+"写入Hbase"+ rowKey)
    })
    println("start to put value")
    table.put(new_row)
    println("finished to put the value")

  }
  def deleteColumn(tableName:String,rowKey:String, family:String,column:Array[(String)]): Unit ={
    val table =getTable(tableName)
    val new_row  = new Delete(Bytes.toBytes(rowKey))
    column.map(x=>{
      if(!(x.isEmpty)){
//        new_row.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
        new_row.deleteColumns(Bytes.toBytes(family),Bytes.toBytes(x))
      }

    })
    table.delete(new_row)
  }
  def main(args: Array[String]) {

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "31818")
    conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver")
    conf.set("zookeeper.znode.rootserver", "rs");
    //conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");
    conf.set("zookeeper.znode.parent", "/hbase");
    conf.set("hbase.client.retries.number", "0")
    //conf.set("hbase.rpc.timeout", "1")

    conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");
    conf.set("hbase.cluster.distributed", "true")
    conf.set("hbase.tmp.dir", "/data/hadoop/data9/hbase-tmp")

    println(conf.get("zookeeper.znode.rootserver"))
    println(conf.get("hbase.zookeeper.quorum"))

    val conn = ConnectionFactory.createConnection(conf)

    /*val admin = conn.getAdmin
    val clusterStatus = admin.getClusterStatus
    admin.close()

    val dead_server = clusterStatus.getDeadServerNames
    val server= clusterStatus.getServers;*/

    try{
      //获取 user 表

      val userTable = TableName.valueOf("GalaxyContent")
      val table = conn.getTable(userTable)

      try{
        //准备插入一条 key 为 id001 的数据
        val p = new Put("001".getBytes)
        //为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
        p.addColumn("value".getBytes,"content".getBytes, "xxxxxxxxxx".getBytes)
        //提交
        table.put(p)


      }finally {
        if(table != null) table.close()
      }

    }finally {
      conn.close()
    }
  }
//  val family = "F"
}
