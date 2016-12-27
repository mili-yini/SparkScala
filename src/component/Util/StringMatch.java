package component.Util;

//import scala.collection.mutable.HashMap;
//import scala.collection.mutable.HashTable;

import com.sun.tools.corba.se.idl.InterfaceGen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sunhaochuan on 2016/12/26.
 */
class Token {
    public int idx;
    public int location;
    public int length;
}

class entry {
    public String name;
    public String label;
}

public class StringMatch {
    HashMap<String, ArrayList<Token> > table_;
    ArrayList<entry> entries_;

    static int token_size_ = 2;

    public void LoadFile(String file)  {

        if (file == null || file.length() == 0) {
            return;
        }

        table_.clear();
        entries_.clear();
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                String[] items = line.split("\t");
                if (items.length != 2) {
                    System.exit(-1);
                }
                entry e = new entry();
                e.name = items[0];
                e.label = items[1];
                AddOneItem(e);
            }
        }catch(Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void AddEntries(List<entry> list) {
        table_.clear();
        entries_.clear();

        for (entry e : list) {
            AddOneItem(e);
        }
    }

    private void AddItemToMap(String key, Token token) {
        if (table_.containsKey(key)) {
            table_.get(key).add(token);
        } else {
            ArrayList<Token> list = new ArrayList<Token>();
            list.add(token);
            table_.put(key, list);
        }
    }
    private void AddOneItem(entry item) {
        entries_.add(item);
        for (int i = 0, idx = 0; i < item.name.length(); i = i + token_size_, idx ++) {
            String token_key = null;
            Token token = new Token();
            token.idx = entries_.size() - 1;
            token.location = idx;
            token.length = item.name.length() / token_size_ + (item.name.length() % token_size_ != 0 ? 1 : 0);
            if (i + token_size_ <= item.name.length() - 1) {
                token_key = item.name.substring(i, i + token_size_ - 1);
                AddItemToMap(token_key, token);
            } else {
                token_key = item.name.substring(i, item.name.length() - 1);
                AddItemToMap(token_key, token);
            }
        }
    }

    public List<Integer> Match(String context) {
        List<Integer> res = new ArrayList<Integer>();

        /*HashMap<Integer, Integer> cur_hold = new HashMap<Integer, Integer>();

        for (int i = 0; i < context.length(); i = i + token_size_) {
            for (int j = 0; j < token_size_ && i + j < context.length(); ++j) {
                ArrayList<Token> list = table_.get(context.substring(i, i + j));
                MatchOneItem(cur_hold, list, res, j < token_size_ - 1);
            }
        }*/
        List<Map<Integer, Integer>> cur_hold=new ArrayList<Map<Integer, Integer>>(token_size_);
        for (int i = 0; i < cur_hold.size(); ++i) {
            cur_hold.add(new HashMap<Integer, Integer>());
        }
        for (int i = 0; i < context.length(); ++i) {
            int shard = i % token_size_;

            for (int j = 0; j < token_size_ && i + j < context.length(); ++j) {
                ArrayList<Token> list = table_.get(context.substring(i, i + j));
                MatchOneItem(cur_hold.get(shard), list, res, j < token_size_ - 1);
            }
        }

        return  res;
    }

    private void MatchOneItem(Map<Integer, Integer> map, List<Token> tokens, List<Integer> res, boolean force_end) {
        HashMap<Integer, Integer> tmp_map = new HashMap<Integer, Integer> ();
        for (Token token : tokens) {
            if (token.location == token.length - 1 || force_end) {
                if (token.location == 0) {
                    res.add(token.idx);
                } else {
                    Integer cur_match = map.get(token.idx);
                    if (cur_match != null && cur_match == token.location - 1) {
                        res.add(token.idx);
                    }
                }
            } else {
                if (token.location == 0) {
                    tmp_map.put(token.idx, 0);
                }
                Integer cur_match = map.get(token.idx);
                if (cur_match != null && cur_match == token.location - 1) {
                    tmp_map.put(token.idx, token.location);
                }
            }
        }
        map = tmp_map;
    }

}
