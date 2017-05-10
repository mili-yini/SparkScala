package Component.Util;

//import scala.collection.mutable.HashMap;
//import scala.collection.mutable.HashTable;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;
import scala.*;
import scala.Serializable;

import java.io.*;
import java.lang.Float;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sunhaochuan on 2016/12/26.
 */


public class StringMatch implements scala.Serializable{

    public class StringInfo implements scala.Serializable {
        public HashMap<String, Float> category_info = new  HashMap<String, Float>();
        public int total_count = 0;
        public int matche_count = 0;
        String tag;

        void Parse(String line) {
            String[] arr = line.split("\t");
            tag = arr[0];
            String[] cats = arr[1].split(",");
            for (String cat : cats) {
                String[] kv = cat.split(":");
                category_info.put(kv[0], Float.parseFloat(kv[1]));
            }
            total_count = Integer.parseInt(arr[2]);
            matche_count = Integer.parseInt(arr[3]);
        }
    }

    public static StringMatch sm =  new StringMatch();
    public  HashMap<String, StringInfo> sm_info = new HashMap<String, StringInfo>();
    public void LoadInfo(String file_name) {
        if (file_name == null || file_name.length() == 0) {
            return;
        }
        try {
            BufferedReader br = new BufferedReader(new FileReader(file_name));
            String line;
            while ((line = br.readLine()) != null) {
                StringInfo si = new StringInfo();
                si.Parse(line);
                sm_info.put(si.tag, si);
            }
        }catch(Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }


    // used for the matching
    public class Token implements Serializable {
        public int idx;
        public int location;
        public int length;
    }

    // every item wich is used to match
    public class entry implements  Serializable {
        public String sub_name;
        public List<Integer> related_IPLIST;
    }
    // the latest matched target
    public class InterestPoint implements Serializable {
        public String label;
        public int weight;
        public String name;
        public List<Integer> total_entry;
    }



    public HashMap<String, ArrayList<Token> > table_;
    public ArrayList<entry> entries_;
    public HashMap<String, Integer> entry_idx_;
    public ArrayList<InterestPoint> IPList_;

    public StringMatch () {
        table_ = new HashMap<String, ArrayList<Token> >();
        entries_ = new ArrayList<entry>();
        entry_idx_ = new  HashMap<String, Integer>();
        IPList_ = new ArrayList<InterestPoint>();
    }

    static int token_size_ = 2;

    public void BuildByMultiFile(String folder) throws FileNotFoundException, IOException {
        if (folder == null || folder.length() == 0) {
            return;
        }
        File file = new File(folder);
        if (!file.isDirectory()) {
            return;
        }
        String[] filelist = file.list();
        for (int i = 0; i < filelist.length; i++) {
            String label = filelist[i].substring(0, filelist[i].length() - 4);
            //File readfile = new File(folder + "//" + filelist[i]);
            LoadFileWithOneLable(folder + "\\" + filelist[i], label);
        }
    }

    // used to add the tag not need to word break
    public void LoadOneItemIP(String file, String label, Integer limit ) {
        if (file == null || file.length() == 0) {
            return;
        }
        int idx = 1;
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String s;
            while ((s = br.readLine()) != null) {
                InterestPoint ip = new InterestPoint();
                ip.label = label;
                ip.total_entry = new ArrayList<Integer>();
                IPList_.add(ip);

                String[] items = s.split("\t");
                ip.name = items[0];
                ip.weight = idx;
                idx++;
                Integer entry_idx = AddOneItem(items[0], ip, IPList_.size() - 1 );
                ip.total_entry.add(entry_idx);
                if (idx >= limit) {
                    break;
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }
    public void LoadOneItemIP(List<String> list, String label, Integer limit ) {


        int idx = 1;
        for (String s : list) {
            InterestPoint ip = new InterestPoint();
            ip.label = label;
            ip.total_entry = new ArrayList<Integer>();
            IPList_.add(ip);

            String[] items = s.split("\t");
            ip.name = items[0];
            ip.weight = idx;
            idx++;
            Integer entry_idx = AddOneItem(items[0], ip, IPList_.size() - 1 );
            ip.total_entry.add(entry_idx);
            if (idx >= limit) {
                break;
            }
        }
    }

    public void LoadTagIP(List<String> list) {
        for (String s : list) {
            InterestPoint ip = new InterestPoint();
            ip.label = s;
            ip.total_entry = new ArrayList<Integer>();
            IPList_.add(ip);

            String[] items = s.split("\t");
            ip.name = items[0];
            ip.weight = 1;
            Integer entry_idx = AddOneItem(items[0], ip, IPList_.size() - 1 );
            ip.total_entry.add(entry_idx);
        }
    }

    public  void LoadMultiItemIP(String file, String label, int name_idx, Integer limit) {
        if (file == null || file.length() == 0) {
            return;
        }
        int idx = 1;
        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String s;
            while ((s = br.readLine()) != null) {
                String[] items = s.split("\t");
                if (items.length < 4) {
                    continue;
                }
                InterestPoint ip = new InterestPoint();
                ip.label = label;
                ip.total_entry = new ArrayList<Integer>();
                IPList_.add(ip);


                ip.name = items[name_idx];
                ip.weight =  Integer.parseInt(items[0]);
                idx ++;
                //List<Term> temp= NlpAnalysis.parse(ip.name);
                List<String> temp = MakeTripleList(ip.name);
                for (String t : temp) {
                    Integer entry_idx = AddOneItem(t, ip, IPList_.size() - 1);
                    ip.total_entry.add(entry_idx);
                }
                if (idx >= limit) {
                    break;
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    public  void LoadMultiItemIP(List<String> list, String label, int name_idx, Integer limit) {
        int idx = 1;
        for (String s : list) {
            String[] items = s.split("\t");
            if (items.length < 4) {
                continue;
            }
            InterestPoint ip = new InterestPoint();
            ip.label = label;
            ip.total_entry = new ArrayList<Integer>();
            IPList_.add(ip);


            ip.name = items[name_idx];
            ip.weight =  Integer.parseInt(items[0]);
            idx ++;
            //List<Term> temp= NlpAnalysis.parse(ip.name);
            List<String> temp = MakeTripleList(ip.name);
            for (String t : temp) {
                Integer entry_idx = AddOneItem(t, ip, IPList_.size() - 1);
                ip.total_entry.add(entry_idx);
            }
            if (idx >= limit) {
                break;
            }
        }
    }

    public List<String> MakeTripleList(String line) {
        List<String> res = new ArrayList<String>();
        if (line.length() <= 3) {
            res.add(line);
            return res;
        }
        for (int i = 0; i < line.length() - 1; ++i) {
            res.add(line.substring(i, i +  2));
        }
        return res;
    }


    public void LoadFileWithOneLable(String file_path, String type)  throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader(file_path));
        String line;
        while ((line = br.readLine()) != null) {
            InterestPoint ip = new InterestPoint();
            ip.label = type;
            ip.total_entry = new ArrayList<Integer>();
            IPList_.add(ip);
            String[] items = line.split("\t");
            ip.name = items[0];
            Integer entry_idx = AddOneItem(items[0], ip, IPList_.size() - 1 );
            ip.total_entry.add(entry_idx);

            //e.related_list.add(IPList_.size() - 1);
        }
    }

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
                InterestPoint ip = new InterestPoint();
                ip.label = line;
                ip.total_entry = new ArrayList<Integer>();
                IPList_.add(ip);

                String[] items = line.split("\t");
                ip.name = items[0];
                ip.weight = 1;
                Integer entry_idx = AddOneItem(items[0], ip, IPList_.size() - 1 );
                ip.total_entry.add(entry_idx);
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
            //AddOneItem(e);
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
    private Integer AddOneItem(String sub_name, InterestPoint ip, Integer ip_idx) {
        Integer entry_idx = null;
        entry item = null;
        if (entry_idx_.containsKey(sub_name) == true) {
            entry_idx = entry_idx_.get(sub_name);
            item = entries_.get(entry_idx);
            item.related_IPLIST.add(ip_idx);
            int i = 0;
            i++;
        } else {
            item = new entry();
            item.sub_name = sub_name;
            item.related_IPLIST = new ArrayList<Integer>();
            item.related_IPLIST.add(ip_idx);
            entries_.add(item);
            entry_idx = entries_.size() - 1;
            entry_idx_.put(sub_name, entry_idx);

            for (int i = 0, idx = 0; i < item.sub_name.length(); i = i + token_size_, idx ++) {
                String token_key = null;
                Token token = new Token();
                token.idx = entry_idx;
                token.location = idx;
                token.length = item.sub_name.length() / token_size_ + (item.sub_name.length() % token_size_ != 0 ? 1 : 0);
                if (i + token_size_ <= item.sub_name.length() - 1) {
                    token_key = item.sub_name.substring(i, i + token_size_);
                    AddItemToMap(token_key, token);
                } else {
                    token_key = item.sub_name.substring(i, item.sub_name.length());
                    AddItemToMap(token_key, token);
                }
            }
        }

        return  entry_idx;
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
        List<HashMap<Integer, Integer>> cur_hold=new ArrayList<HashMap<Integer, Integer>>();
        for (int i = 0; i < token_size_; ++i) {
            cur_hold.add(new HashMap<Integer, Integer>());
        }
        //System.out.println(cur_hold.size());
        for (int i = 0; i < context.length(); i = i + 1) {
            int shard = i % token_size_;

            for (int j = 0; j < token_size_ && i + j < context.length(); ++j) {
                String cur_key = context.substring(i, i + j + 1);
                ArrayList<Token> list = table_.get(cur_key);
                if (list != null) {
                    MatchOneItem(cur_hold.get(shard), list, res, j < token_size_ - 1);
                }
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
                } else {
                    Integer cur_match = map.get(token.idx);
                    if (cur_match != null && cur_match == token.location - 1) {
                        tmp_map.put(token.idx, token.location);
                    }
                }
            }
        }
        //map = tmp_map;
        map.clear();
        for (Map.Entry<Integer, Integer> kv : tmp_map.entrySet()) {
            map.put(kv.getKey(), kv.getValue());
        }
    }

    public static void main(String args[]) {
        System.out.println("Hello World!");
        StringMatch stringMatch = new StringMatch();
        String directory_path = "D:\\Temp\\user_dic\\user_dic";
        try {
            stringMatch.BuildByMultiFile(directory_path);
        } catch( Exception e) {

        }

        String content = "媒体：南海一度战云密布 火箭军数十枚导弹引弓待发";
        List<Integer> res = stringMatch.Match(content);
        for (Integer i : res) {
            entry e = stringMatch.entries_.get(i);
            for (Integer ip_idx : e.related_IPLIST) {
                System.out.println(e.sub_name + ":" +stringMatch.IPList_.get(ip_idx).label);
            }
        }

    }

}
