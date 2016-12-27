package Component.nlp;

import scala.collection.immutable.Range;

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
    List<String[]> spliteSentences=new ArrayList<String[]>();
    String[] spliteTitle=null;
    List<String> keyWords=new ArrayList<String>();
    BigInteger simHash=new BigInteger("-1");
    public void getTF(){
        for(Sentence sentence:this.sentences){
            for(Word word:sentence.getWords()){
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
        String[] ss=text.split(dot);
        for(String sentence:ss){
            Sentence sen=new Sentence(sentence);
            sentences.add(sen);
            spliteSentences.add(sen.spliteSentence);
        }
        wordTextRank=TextRank.getTextRank(this);
        getTF();
        this.simHash=SimHash.simHash(this.tf,128);
        //处理title
        Sentence sen=new Sentence(title);
        spliteTitle=sen.spliteSentence;
        for(Word word:sen.getWords()){
            String nature=word.getNature();
            boolean flag1=nature.equals("nr")||nature.equals("ns")||nature.equals("nz");
            keyWords.add(word.getText());
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
