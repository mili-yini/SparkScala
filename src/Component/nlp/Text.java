package Component.nlp;
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
    List<String> keyWords=new ArrayList<String>();
    BigInteger simHash=new BigInteger("-1");
    public void addComopsticDoc(CompositeDoc doc){
        //添加textrank
        for(String word:wordTextRank.keySet()){
            short value=(short)(wordTextRank.get(word)*100);
            ItemFeature iF=new ItemFeature();
            iF.setWeight(value);
            iF.setName(word);
            iF.setType(FeatureType.LABEL);

            doc.feature_list.add(iF);
        }
        //添加tf
        for(String word:tf.keySet()){
            short value=(short)(tf.get(word)*100);
            ItemFeature iF=new ItemFeature();
            iF.setWeight(value);
            iF.setName(word);
            doc.text_rank.add(iF);
        }
       //添加分词后的正文句子
        for(String sentence:this.spliteSentences){
            doc.body_words.add(sentence);
        }
        //添加分词后的标题
        doc.title_words.add(this.spliteTitle);
        //添加simhash
        int j=this.simHash.intValue();
        doc.media_doc_info.setName_fingerprint(j);
        //添加关键词

    }
    public void getTF(){
        for(Sentence sentence:this.sentences){
            for(Word word:sentence.getWords()){
                if(!word.getNature().startsWith("n")){
                    continue;
                }
                if(word.getNature().equals("null")){
                    continue;
                }
                String text=word.getText();
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

        String[] ss=null;
        if(text!=null){
           ss=text.split(dot);
        }else{
            ss=new String[1];
            ss[0]=title;
        }
        //处理正文 存储分词后的句子
        for(String sentence:ss){
            Sentence sen=new Sentence(sentence);
            sentences.add(sen);
            spliteSentences.add(sen.spliteSentence);
        }
        //处理title
        Sentence titlesen=new Sentence(title);
        this.titleSentence=titlesen;
        this.spliteTitle=titlesen.spliteSentence;
        //计算textrank
        wordTextRank=TextRank.getTextRank(this);
        //计算tf
        getTF();
        this.simHash=SimHash.simHash(this.tf,128);
        for(Sentence sen:this.sentences){
          for(Word word:sen.getWords()){
            String nature=word.getNature();
            boolean flag1=nature.equals("nr")||nature.equals("ns")||nature.equals("nz");
            if(flag1){
               keyWords.add(word.getText());
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

}
