package Component.DocumentProcess

import scala.collection.JavaConversions._
import java.text.SimpleDateFormat
import java.util.Date
import javax.naming.Context

import Component.HBaseUtil.HbashBatch
import Component.nlp.Text
import CompositeDocProcess.DocumentAdapter
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import pipeline.CompositeDoc
import shared.datatypes.ItemFeature

/**
  * Created by sunhaochuan on 2016/12/28.
  */
object DocumentProcess {
  // define the different interface to streaming and batch
  def ProcessStream( documents : RDD[(String, String)]) : RDD[(String, String, String)] = {
    Process(documents)
  }
  def ProcessBatch( documents : RDD[(Object, BSONObject)]) : RDD[(String, String, String)] = {
    Process(documents.map(document => {(document._1.toString(), document._2.toString())}))
  }
  def Process(documents : RDD[(String, String)]) : RDD[(String, String, String)] = {

    val processedRDD = documents.map(line => {
      val doc: CompositeDoc = DocumentAdapter.FromJsonStringToCompositeDoc(line._2);

      //
      var serialized_string: String = null;
      var id :String = null;
      var date_prefix: String = null;
      if (doc != null) {
        //add by lujing
        val text=new Text(doc.media_doc_info.name,doc.description)
        text.addComopsticDoc(doc)

        // add the feature list to media doc info
        doc.feature_list.map(e=>doc.media_doc_info.feature_list.put(e.name, e))
        val context: Context = null;
        serialized_string = DocProcess.CompositeDocSerialize.Serialize(doc, context)
        id = doc.media_doc_info.id
        val dateFormat = new SimpleDateFormat("yyMMdd")
        val crawler_time = new Date(doc.media_doc_info.crawler_timestamp)
        date_prefix = dateFormat.format(doc.media_doc_info.crawler_timestamp * 1000 )

      } else {
        System.err.println("Failed to parse :" + line._2)
      }
      (id, serialized_string, date_prefix)
    }).filter( x  => x._1 != null && x._2 != null)

    processedRDD
  }
}
