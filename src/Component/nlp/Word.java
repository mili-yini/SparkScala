package Component.nlp;

import java.io.Serializable;

/**
 * Created by lujing1 on 2016/12/19.
 */
public class Word implements Serializable {
    private String text="";
    private String nature ="";
    private boolean isStopWord;


    public Word(String text, String nature, boolean isStopWord, String entityType) {
        this.text = text;
        this.nature = nature;
        this.isStopWord = isStopWord;

    }

    public String getText() {
        return text;
    }

    public String getNature() {
        return nature;
    }

    public boolean isStopWord() {
        return isStopWord;
    }


    public String toString(){
        return this.text+"("+","+this.getNature()+","+this.isStopWord()+")";
    }
}
