package Component.nlp

import java.io.StringReader
import java.util


import org.ansj.splitWord.analysis.{BaseAnalysis, NlpAnalysis, ToAnalysis}


/**
  * Created by lujing1 on 2016/12/19.
  */
object Test {
  def main(args: Array[String]): Unit = {
     val s1=new Text("辜鸿铭","辜鸿铭是我国近代中西文化交流史上的一位复杂而传奇的人物。他凭借着非凡的语言天赋和对中国传统文化的赤诚热爱，为中国文化的对外输出做出了卓越的历史贡献，这对以向西方学习为主要趋向的近代中国来说，尤为难能可贵。辜鸿铭在世时，以其怪诞的外表和奇异的言行博尽了世人的眼球，成为一道可以与紫禁城相媲美的“文化景观”，受到了西方世界的极大关注。辜鸿铭逝世后，作为同样在中西文化交流中成就斐然的学者、翻译家林语堂，专门在其主编的《人间世》杂志上推出“辜鸿铭特辑”（1934年第12期），并亲自撰写多篇回忆文章以示纪念。上世纪九十年代以来，极具历史复杂性的辜鸿铭逐渐进入学界的研究视野，《文化怪杰辜鸿铭》等具有代表性的研究著作相继问世，从历史学的角度对辜鸿铭生前的奇闻异事做出了深入理性的阐释。随后，辜氏的西文著作不断在黄兴涛等学者的组织下被译成中文集结出版。在学界所引起的关注也促使“辜鸿铭热”成为了一个持续的文化现象。而近十余年来，一则关于“辜鸿铭与泰戈尔同被提名为1913年诺贝尔文学奖候选人”的说法在网络上流传甚广，人民网、中国社会科学网等主流网站均有采纳，在一些正规的学术著作中也偶有提及。煞有介事的论述无疑为辜鸿铭的生平轶事再添一抹传奇，一度形成了三人成虎般的热闹态势。而辜鸿铭也由此被错误地捧上了“中国历史上第一个获得诺贝尔文学奖提名”的历史圣坛，深深寄寓着国人的骄傲和对诺奖的殷切期待。")
    println(s1.simHash)



  }
}
