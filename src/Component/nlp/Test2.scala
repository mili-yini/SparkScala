package Component.nlp

import ldacore.CalLDA
import org.apache.spark.mllib.clustering.LocalLDAModel
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Created by lujing1 on 2016/12/20.
  */
object Test2 {
  def main(args: Array[String]): Unit = {
//    val text1=new Text("辜鸿铭","辜鸿铭是我国近代中西文化交流史上的一位复杂而传奇的人物。他凭借着非凡的语言天赋和对中国传统文化的赤诚热爱，为中国文化的对外输出做出了卓越的历史贡献，这对以向西方学习为主要趋向的近代中国来说，尤为难能可贵。辜鸿铭在世时，以其怪诞的外表和奇异的言行博尽了世人的眼球，成为一道可以与紫禁城相媲美的“文化景观”，受到了西方世界的极大关注。辜鸿铭逝世后，作为同样在中西文化交流中成就斐然的学者、翻译家林语堂，专门在其主编的《人间世》杂志上推出“辜鸿铭特辑”（1934年第12期），并亲自撰写多篇回忆文章以示纪念。上世纪九十年代以来，极具历史复杂性的辜鸿铭逐渐进入学界的研究视野，《文化怪杰辜鸿铭》等具有代表性的研究著作相继问世，从历史学的角度对辜鸿铭生前的奇闻异事做出了深入理性的阐释。随后，辜氏的西文著作不断在黄兴涛等学者的组织下被译成中文集结出版。在学界所引起的关注也促使“辜鸿铭热”成为了一个持续的文化现象。而近十余年来，一则关于“辜鸿铭与泰戈尔同被提名为1913年诺贝尔文学奖候选人”的说法在网络上流传甚广，人民网、中国社会科学网等主流网站均有采纳，在一些正规的学术著作中也偶有提及。煞有介事的论述无疑为辜鸿铭的生平轶事再添一抹传奇，一度形成了三人成虎般的热闹态势。而辜鸿铭也由此被错误地捧上了“中国历史上第一个获得诺贝尔文学奖提名”的历史圣坛，深深寄寓着国人的骄傲和对诺奖的殷切期待。")
//    val text2=new Text("习近平","习近平强调，要始终重视“三农”工作，持续强化重农强农信号；要准确把握新形势下“三农”工作方向，深入推进农业供给侧结构性改革；要在确保国家粮食安全基础上，着力优化产业产品结构；要把发展农业适度规模经营同脱贫攻坚结合起来，与推进新型城镇化相适应，使强农惠农政策照顾到大多数普通农户；要协同发挥政府和市场“两只手”的作用，更好引导农业生产、优化供给结构；要尊重基层创造，营造改革良好氛围。")
//    val conf = new SparkConf()
//    conf.setAppName("Test")
//    conf.setMaster("local[3]")
//    val sc = new SparkContext(conf)
//    val rdd=sc.parallelize(Seq((text1),(text2)))
//   //.mapValues(_.getLDAFeature.toArray.toList.map(e=>(e._1,e._2.toDouble)))
//    //val result=CalLDA.getLDAModel[Int](sc,rdd,2)
////    result._1.foreach(println(_))
//    val result=Word2Vector.getEntityRelation(rdd)
//    result.foreach(println(_))
  }

}
