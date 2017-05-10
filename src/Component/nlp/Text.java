package Component.nlp;
import org.apache.commons.collections.KeyValue;
import pipeline.CompositeDoc;
import scala.collection.immutable.Range;
import serving.mediadocinfo.MediaDocInfo;
import shared.datatypes.FeatureType;
import shared.datatypes.ItemFeature;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by lujing1 on 2016/12/19.
 */
public class Text implements Serializable {
    static String dot="[,.，。；！？]";
    List<Sentence> sentences=new ArrayList<Sentence>();
    Map<String,Double> wordTextRank=new HashMap<String,Double>();
    Map<String,Double> tf=new HashMap<String,Double>();
    List<String> spliteSentences=new ArrayList<String>();
    String spliteTitle=null;
    Sentence titleSentence=null;
    // 用来存放分词器中输出的命名实体
    List<String> keyWords=new ArrayList<String>();
    BigInteger simHash=new BigInteger("-1");
    List<String> keyInDict = new ArrayList<String>();
    // used to add the NLP analyze result to CompositeDoc
    public void addComopsticDoc(CompositeDoc doc){
        //添加textrank
        /*for(String word:wordTextRank.keySet()){
            short value=(short)(wordTextRank.get(word)*100);
            ItemFeature iF=new ItemFeature();
            iF.setWeight(value);
            iF.setName(word);
            iF.setType(FeatureType.LABEL);

            doc.text_rank.add(iF);
        }*/
        //添加tf
        /*for(String word:tf.keySet()){
            short value=(short)(tf.get(word)*100);
            ItemFeature iF=new ItemFeature();
            iF.setWeight(value);
            iF.setName(word);
            doc.text_rank.add(iF);
        }*/
       //添加分词后的正文句子
        for(String sentence:this.spliteSentences){
            doc.body_words.add(sentence);
        }
        //添加分词后的标题
        doc.title_words.add(this.spliteTitle);
        //添加simhash
        long j=this.simHash.longValue();
        doc.media_doc_info.setName_fingerprint(j);
        //添加关键词

        //添加林鹏词库
        doc.title_np = new ArrayList<String>() ;
        for (String key_word : keyInDict) {
            doc.title_np.add(key_word);

            /*ItemFeature iF=new ItemFeature();
            short value = 1;
            iF.setWeight(value);
            iF.setName(key_word);
            //iF.setType(FeatureType.NP);
            iF.setType(FeatureType.TAG);
            doc.feature_list.add(iF);*/
        }
        //添加分词器中的命名实体
        doc.title_nnp = new ArrayList<String>();
        for (String key_word : keyWords) {
            doc.title_nnp.add(key_word);
        }
    }
    public void getTF(){
        List<Sentence> temp=new ArrayList<Sentence>();
        temp.addAll(this.sentences);
        temp.add(this.titleSentence);
        for(Sentence sentence:temp){
            for(Word word:sentence.getWords()){
                String text=word.getText();
                if(text.length()<2){
                    continue;
                }
                if(!tf.containsKey(text)){
                    tf.put(text,0.0);
                }
                tf.put(text,tf.get(text)+1);
            }
        }
    }
    public Map<String,Double>getLDAFeature(){
        Map<String,Double> result=new HashMap<String,Double>();
        Set<String> set=new HashSet<String>();
        set.addAll(tf.keySet());
        set.retainAll(wordTextRank.keySet());
        for(String word:set){
            result.put(word,wordTextRank.get(word)*tf.get(word));
        }
        return result;
    }
    public Text(String title,String text) throws IOException {
        //处理正文
        // ss代表正文
        String[] ss=null;
        if(text!=null){
            //将正文通过标点符号分句子
           ss=text.split(dot);
        }else{
            ss=new String[1];
            ss[0]=title;
        }
        //处理正文 存储分词后的句子
        for(String sentence:ss){
            // sentence 字符串转化为自定义的类型，包含分词
            Sentence sen=new Sentence(sentence);
            sentences.add(sen);
            spliteSentences.add(sen.spliteSentence);
        }
        //处理title
        Sentence titlesen=new Sentence(title);
        this.titleSentence=titlesen;
        this.spliteTitle=titlesen.spliteSentence;
        //计算textrank
        //wordTextRank=TextRank.getTextRank(this);
        //计算tf
        //getTF();
        this.simHash=Text.GetSimHash(title);
        // 从分词器的结果中取人名，地名，机构名，等作为命名实体
        for(Sentence sen:this.sentences){
          for(Word word:sen.getWords()){
            // nature 代表word的词性
            String nature=word.getNature();
            boolean flag1=nature.equals("nr")||nature.equals("ns")||nature.equals("nz");
            if(flag1){
               keyWords.add(word.getText());
            }
            boolean flag_user_dict = nature.equals("userDefine");
              if (flag_user_dict) {
                  keyInDict.add(word.getText());
              }
          }
        }
    }
    public String toValue(){
        StringBuffer sb=new StringBuffer();
        for(Sentence sentence:sentences){
            sb.append(sentence.toString()).append("\n");
        }
        for(String word:wordTextRank.keySet()){
            sb.append(word).append(":").append(wordTextRank.get(word)).append(" ");
        }
        sb.append("\n");
        for(String word:tf.keySet()){
            sb.append(word).append(":").append(tf.get(word)).append(" ");
        }
        sb.append("\n");
        return sb.toString();
    }

    public String Debug() {
        StringBuffer sb = new StringBuffer();
        sb.append("text rank: ");
        for (String word :wordTextRank.keySet()
             ) {
            sb.append(word + " ");
        }
        sb.append("\n");

        sb.append("entity: ");
        for (String word : keyWords) {
            sb.append(word + " ");
        }
        sb.append("\n");

        sb.append("spliteTitle: ");
        sb.append(this.spliteTitle);
        sb.append("\n");

        return sb.toString();
    }

    public static String black_list = " \t\n\r~!@#$%^&*()_+{}|:\"<>?-=[]\\;',./ ！@#￥%……&*（）——+{}|：“《》？-=【】、；‘，。、";
    public static String black_list1 = " \t";

    public static BigInteger GetSimHash(String title) {
        Map<String,Double> two_gram = new HashMap<String,Double>();
        for (int i = 0; i < title.length() - 1 ; ++i) {
            if (black_list.indexOf(title.charAt(i)) != -1) {
                continue;
            }
            if (black_list.indexOf(title.charAt(i + 1)) != -1) {
                continue;
            }
            String tmp = title.substring(i, i + 2);
            if (two_gram.containsKey(tmp)) {
                two_gram.put(tmp, two_gram.get(tmp) + 1.0d);
            } else {
                two_gram.put(tmp, 1.0d);
            }
        }

        return SimHash.simHash(two_gram,64);
    }

    public static Map<String,Double> GetCosin(String title) {
        Map<String,Double> two_gram = new HashMap<String,Double>();
        for (int i = 0; i < title.length() - 1 ; ++i) {
            if (black_list.indexOf(title.charAt(i)) != -1) {
                continue;
            }
            if (black_list.indexOf(title.charAt(i + 1)) != -1) {
                continue;
            }
            String tmp = title.substring(i, i + 2);
            if (two_gram.containsKey(tmp)) {
                two_gram.put(tmp, two_gram.get(tmp) + 1.0d);
            } else {
                two_gram.put(tmp, 1.0d);
            }
        }
        return two_gram;
    }

    public static double Jaccord(Map<String,Double> a, Map<String,Double> b) {

        double score = 0.0;
        for (Map.Entry<String, Double> kv : a.entrySet()) {
            if (b.containsKey(kv.getKey())) {
                score = score + b.get(kv.getKey()) * kv.getValue();
            }
        }
        double mode_a = 0.0;
        for (Map.Entry<String, Double> kv : a.entrySet())  {
            mode_a = mode_a + kv.getValue() * kv.getValue();
        }
        double model_b = 0.0;
        for (Map.Entry<String, Double> kv : b.entrySet()) {
            model_b = model_b + kv.getValue() * kv.getValue();
        }

        return score/ Math.sqrt(mode_a*model_b);
    }

    public static void main(String[] args)
    {
        String title1 = "李治廷曝剧组小狗被撞死发文希望司机能承认错误";
        String title2 = "李治廷曝剧组小狗翠花被撞死希望司机能承认错误";
        BigInteger l1 = Text.GetSimHash(title1);
        BigInteger l2 = Text.GetSimHash(title2);
        System.out.println(l1);
        System.out.println(l2);
        System.out.println(SimHash.hammingDistance(l1, l2, 64));
        double s = Jaccord(GetCosin(title1), GetCosin(title2));
        System.out.println(s);
    }

}
