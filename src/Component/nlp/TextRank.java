package Component.nlp;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by lujing1 on 2016/12/20.
 */
public class TextRank {
    static public Map<String,Double>getTextRank(Text text){
        Map<String,Set<String>> wordRelation=getWordRelation(text);
       Map<String,Double> result=calWordsRank(wordRelation);
        return result;
    }
    static public Map<String,Set<String>>getWordRelation(Text text){
        Map<String,Set<String>> result=new HashMap<String,Set<String>>();
        for(Sentence sentence:text.sentences){
            Set<String> words=new HashSet<String>();
            for(Word word:sentence.getWords()){
                 String nature=word.getNature();
                 boolean flag1=nature.equals("nr")||nature.equals("ns")||nature.equals("nz");

                if(flag1){
                    words.add(word.getText());
                }
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
