package Component.nlp

//import org.ansj.library.DicLibrary
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.nlpcn.commons.lang.tire.domain.Forest
import org.nlpcn.commons.lang.tire.library.Library
import pipeline.CompositeDoc

/**
  * Created by sunhaochuan on 2017/2/14.
  */
object ANSJ {
  /*def LoadHDFSDictOnDriver(sc: SparkContext, userDic: String) :  Broadcast[Forest]  = {
    val forest:Forest = new Forest()
    val uDic = sc.wholeTextFiles(userDic)

    val directories = uDic.collect()
    for(i <-0 until directories.length) {
      val filenameContent = directories(i)
      sc.parallelize(filenameContent._2.split("\n")).collect().foreach{x =>
        Library.insertWord(forest, x)
      }
    }

    val b_forest = sc.broadcast(forest)
    b_forest
  }

  def AddLibraryDistribute(b_forest:Broadcast[Forest]) : Boolean = {

    DicLibrary.put(DicLibrary.DEFAULT, DicLibrary.DEFAULT, b_forest.value)

    true
  }

  def ProcessCompositeDoc(doc: CompositeDoc, b_forest:Broadcast[Forest]) : Boolean = {

    AddLibraryDistribute(b_forest)



    true
  }*/
}

