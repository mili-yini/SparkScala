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
      private List<Word> words=new ArrayList<Word>();
    String[] spliteSentence=null;
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
        instertTokenWord(sentence);
        List<String> keys = new ArrayList<String>();
        List<Term> temp= NlpAnalysis.parse(sentence);
        StringBuffer sb=new StringBuffer();
        for(Term word:temp){
            int languageId=1;
            words.add(new Word(word.getName(),word.getNatrue().natureStr,false,""));
            sb.append(word.getName()).append(" ");
        }
        spliteSentence=sb.toString().split(" ");

    }
    public String toString(){
        StringBuffer sb=new StringBuffer();
        for(Word word:this.words){
            sb.append(word.toString()).append(" ");
        }
        return sb.toString();
    }
}
