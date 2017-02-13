package Component.nlp;


import org.ansj.domain.Term;
import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.NlpAnalysis;


import java.io.IOException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by lujing1 on 2016/12/19.
 */
public class Sentence implements Serializable {
    //分词之后的结果
      private List<Word> words=new ArrayList<Word>();
    //分词之后的结果拼接成字符串
    String spliteSentence=null;
    public List<Word> getWords() {
        return words;
    }

    static Map<String,String> parserEntityResult(String input){
        String[] wts=input.split(" ");
        Map<String,String> result=new HashMap<String,String>();
        for(String wt:wts){
            String[] ss=wt.split("/");
            if(ss.length==2){
                result.put(ss[0],ss[1]);
            }
        }
        return result;
    }
    //目前只有一种书名号寻找词的方法，正则匹配
    static public void instertTokenWord(String sentence){
        String regex="《(.*?)》";
        Pattern pattern=Pattern.compile(regex);
        Matcher m=pattern.matcher(sentence);
        while(m.find()){
            String word=m.group(1);
            if(word.length()<10){
                UserDefineLibrary.insertWord(word, "userDefine", 1000);
            }
        }
    }
    public Sentence(String sentence)throws IOException {
        //动态的构造字典
        instertTokenWord(sentence);
        List<String> keys = new ArrayList<String>();
        //调用ansj中科院分词器进行分词
        List<Term> temp= NlpAnalysis.parse(sentence);
        StringBuffer sb=new StringBuffer();
        for(Term word:temp){
            int languageId=1;
            words.add(new Word(word.getName(),word.getNatureStr(),false));
            sb.append(word.getName()).append(" ");
        }
        spliteSentence=sb.toString();

    }
    public String toString(){
        StringBuffer sb=new StringBuffer();
        for(Word word:this.words){
            sb.append(word.toString()).append(" ");
        }
        return sb.toString();
    }
}
