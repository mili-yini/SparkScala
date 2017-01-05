package Component.nlp;

import java.util.*;

/**
 * Created by lujing1 on 2016/12/20.
 */
public class TextRank {
    static public Map<String,Double>getTextRank(Text text){
        Map<String,Set<String>> wordRelation=getWordRelation(text);
       Map<String,Double> result=calWordsRank(wordRelation);
//        if(result.size()==0){
//            System.out.println("a");
//        }
        return result;
    }
    static public Map<String,Set<String>>getWordRelation(Text text){
        Map<String,Set<String>> result=new HashMap<String,Set<String>>();
        List<Sentence> list=new ArrayList<Sentence>();
        list.addAll(text.sentences);
        list.add(text.titleSentence);
        for(Sentence sentence:list){

            Set<String> words=new HashSet<String>();
            for(Word word:sentence.getWords()){
                if(!word.getNature().startsWith("n")){
                    continue;
                }
                if(word.getNature().equals("null")){
                    continue;
                }
                String wordName=word.getText();
                if(wordName.length()<2){
                    continue;
                }
                words.add(word.getText());
            }
            for(String word:words){
                if(!result.containsKey(word)){
                    result.put(word,new HashSet<String>());
                }
                result.get(word).addAll(words);
            }
        }
        return result;
    }

    static public Map<String, Double> calWordsRank(Map<String, Set<String>> words){
        Map<String, Double> score = new HashMap<String, Double>();
        int max_iter=100;
        double min_diff = 0.001f;
        double d=0.1f;
        for (int i = 0; i < max_iter; ++i)
        {
            Map<String, Double> m = new HashMap<String, Double>();
            double max_diff = 0;
            for (Map.Entry<String, Set<String>> entry : words.entrySet())
            {
                String key = entry.getKey();
                Set<String> value = entry.getValue();
                m.put(key, 1 - d);
                for (String other : value)
                {
                    int size = words.get(other).size();
                    if (key.equals(other) || size == 0) continue;
                    m.put(key, m.get(key) + d / size * (score.get(other) == null ? 0 : score.get(other)));
                }
                max_diff = Math.max(max_diff, Math.abs(m.get(key) - (score.get(key) == null ? 0 : score.get(key))));
            }
            score = m;
            if (max_diff <= min_diff) break;
        }
        return score;
    }
}
