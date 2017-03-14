package Component.Util;


import org.apache.commons.codec.binary.Base64;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by sunhaochuan on 2017/3/6.
 */
public class ZhCnWordProcess {

    static public String JoinBody(List<String> main_text_list) {
        if (main_text_list == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < main_text_list.size(); ++i) {
            if (i != 0) {
                sb.append(" ");
            }
            sb.append(main_text_list.get(i));
        }
        return sb.toString();
    }

    static private boolean IsLetter(Character c) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
            return  true;
        }
        return false;
    }


    static public String SplitZHCN(String input) {

        if (input == null) {
            return null;
        }

        ArrayList<String> res = new ArrayList<String>();
        int current = 0;
        for (int i = 0; i < input.length(); ++i) {
            char c = input.charAt(i);
            if (input.charAt(i) == ' ' || input.charAt(i) == '\t' || input.charAt(i) == '\n' || input.charAt(i) == '\r') {
                continue;
            }
            if (input.charAt(i) >= 128) {
                String temp = input.substring(i, i + 1);
                res.add(temp);
            } else if (Character.isDigit(input.charAt(i)) || IsLetter(input.charAt(i)) || input.charAt(i) == '.') {
                if (i > 0 && (Character.isDigit(input.charAt(i - 1 )) ||IsLetter(input.charAt(i - 1)) || input.charAt(i - 1) == '.')) {
                    String temp = res.get(res.size() - 1) + input.charAt(i);
                    res.set(res.size() - 1, temp);
                } else {
                    String temp = input.substring(i, i + 1);
                    res.add(temp);
                }
            } else if (c == '%') {
                if (i > 0 && Character.isDigit(input.charAt(i - 1 ))) {
                    String temp = res.get(res.size() - 1) + input.charAt(i);
                    res.set(res.size() - 1, temp);
                } else {
                    String temp = input.substring(i, i + 1);
                    res.add(temp);
                }
            }
            else {
                String temp = input.substring(i, i + 1);
                res.add(temp);
            }
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < res.size(); ++i) {
            if (i != 0) {
                sb.append(' ');
            }
            sb.append(res.get(i));
        }
        return  sb.toString();
    }

    static public List<String> GetCategory(String raw) {
        List<String> res = new ArrayList<String>();
        String[] kv_pairs = raw.split(";");
        for (String kv : kv_pairs) {
            if (kv.length() == 0) {
                continue;
            }
            String[] pair = kv.split(":", 2);
            try {
                if (Double.parseDouble(pair[1]) >= 0.2) {
                    res.add(pair[0]);
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    static public List<String> GetTag(String raw, Map<String, Integer> match) {
        List<String> res = new ArrayList<String>();
        String[] kv_pairs = raw.split(";");
        if (kv_pairs == null || kv_pairs.length == 0) {
            return res;
        }
        double top_weight = 0.0f;
        for (int i = 0; i < kv_pairs.length; ++i) {
            if (kv_pairs[i].length() == 0) {
                continue;
            }
            String[] pair = kv_pairs[i].split(":", 2);
            String name = pair[0];
            try {
                double weight = Double.parseDouble(pair[1]);
                if (i == 0) {
                    top_weight = weight;
                }

                if (match.get(name) != null) {
                    res.add(name);
                }

                if (i < 5) {
                    if (weight > 0.005 && weight > top_weight / 10) {
                        res.add(name);
                    }
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        return res;
    }


    public static void main(String[] args) {
        BufferedReader br = new BufferedReader(new InputStreamReader( System.in ));
        String line;
        try {
            while ((line = br.readLine()) != null) {
                //String[] sp = line.split("|", 2);
                //String input = Base64.decodeBase64(sp[1]).toString();
                //String label = "__LABEL__" + sp[1];
                String out = SplitZHCN(line);
                System.out.println(out);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        /*String text_so_output = "";
        String title = "";
        String input_hot_data_dir = "D:\\Temp\\toutiao_tag.txt";
        StringMatch sm  = new StringMatch();
        sm.LoadFile(input_hot_data_dir);

        List<Integer> res = sm.Match(title);
        Map<String, Integer> hash_map = new HashMap<String, Integer>();
        for (Integer i : res) {

        }*/

    }
}
