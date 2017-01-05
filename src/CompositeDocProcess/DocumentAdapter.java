package CompositeDocProcess;

import DocProcess.CompositeDocSerialize;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.hadoop.io.Text;
import pipeline.CompositeDoc;
import serving.GlobalIdType;
import serving.mediadocinfo.MediaDocInfo;
import shared.datatypes.DataType;
import shared.datatypes.FeatureType;
import shared.datatypes.ItemFeature;
import shared.datatypes.ProductCode;
import sun.misc.BASE64Decoder;

import javax.naming.Context;
import java.io.*;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

//import static shared.datatypes.ProductCode.INDIA_IMAGETEXT;

/**
 * Created by zhanglin5 on 2016/12/21.
 */
public class DocumentAdapter {
    static public CompositeDoc FromJsonToCompositeDoc(JSONObject one_json) throws ParseException, IOException {
        CompositeDoc compositeDoc = new CompositeDoc();
        MediaDocInfo media_doc = new MediaDocInfo();
        compositeDoc.setFeature_list(new ArrayList<ItemFeature>());
        compositeDoc.setText_rank(new ArrayList<ItemFeature>());
        compositeDoc.setBody_words(new ArrayList<String>());
        compositeDoc.setTitle_words(new ArrayList<String>());
        media_doc.setFeature_list(new HashMap<String, ItemFeature>());
        //mast have
        if (one_json.get("info_id") != null) {
            media_doc.setId(one_json.get("info_id").toString());
        } else {
            return null;
        }
        if (one_json.get("type_id") != null) {
            String type_id = one_json.get("type_id").toString();
            media_doc.setContent_type(Integer.parseInt(type_id));
        }
        if (one_json.get("title") != null) {
        media_doc.setName(one_json.get("title").toString());
        media_doc.setNormalized_name(one_json.get("title").toString());
        } else {
            return null;
         }
        if (one_json.get("url") != null) {
            media_doc.setPlay_url(one_json.get("url").toString());
        } else {
            return null;
        }
        if (one_json.get("update_time") != null) {
            String tmp = one_json.get("update_time").toString();
            Long tmp_long = Long.parseLong(tmp);
            media_doc.setCrawler_timestamp(tmp_long);
            compositeDoc.setCrawl_time(tmp_long);
        } else {
            return null;
        }

        media_doc.setData_type(DataType.WEB_DOCUMENT);
        long doc_id ;
        int mid = media_doc.id.length() / 2;
        long h1 = media_doc.id.substring(0, mid).hashCode();
        long h2 = media_doc.id.substring(mid, media_doc.id.length()).hashCode();
        doc_id = (h1 << 32) + h2;
        compositeDoc.setDoc_id(doc_id);

        long ITEM_ID_BITS = ((long)1L << 56) - 1;
        long type_long = 212;
        long global_id ;
        if (type_long > 100 && type_long <= 163) {
            global_id = (((type_long) - 100) << 56) + (doc_id & ITEM_ID_BITS);
        } else if (type_long > 200 && type_long <= 263) {
            global_id = (((type_long) - 200 + 64) << 56) + (doc_id & ITEM_ID_BITS);
        } else {
            global_id = 0L;
            throw  new IOException("global id is 0!");
        }

        media_doc.setGlobal_id64(global_id);

        //exchange the id
        compositeDoc.setId(media_doc.id);
        media_doc.setId(String.valueOf(type_long) + "_"+ String.valueOf(doc_id));


        // not have
        if (one_json.get("modify_date") != null) {
            String modify_date = one_json.get("modify_date").toString();
            SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd HH:MM:SS");
            Date dt = new Date();
            dt = s.parse(modify_date);
            // getTime 获取的是毫秒
            long t = dt.getTime() / 1000;
            media_doc.setUpdate_timestamp(t);   //
        }
        if (one_json.get("create_time") != null) {
            String creat_date = one_json.get("create_time").toString();
            SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd HH:MM:SS");
            Date dt = new Date();
            dt = s.parse(creat_date);
            // getTime 获取的是毫秒
            long t = dt.getTime() / 1000;
            media_doc.setCreate_timestamp(t);
        }

        if (one_json.get("publish_date") != null) {
            String publish_date = one_json.get("publish_date").toString();
            SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS");
            Date dt = new Date();
            dt = s.parse(publish_date);
            // getTime 获取的是毫秒
            long t = dt.getTime() / 1000;
            media_doc.setContent_timestamp(t);
        } else if (media_doc.isSetCreate_timestamp()) {
            media_doc.setContent_timestamp(media_doc.create_timestamp);
        } else if (media_doc.isSetUpdate_timestamp()) {
            media_doc.setContent_timestamp(media_doc.update_timestamp);
        } else {
            media_doc.setContent_timestamp(media_doc.update_timestamp);
        }

        if (one_json.get("description") != null) {
            String description = one_json.get("description").toString();
            compositeDoc.setDescription(description);
        }
        if (one_json.get("category_id") != null) {
            String category_id = one_json.get("category_id").toString();
            media_doc.setCategory_name(category_id);
            // ADD it to feature list
            ItemFeature item_feature = new ItemFeature();
            item_feature.setName("CATEGORY_"+category_id);
            item_feature.setType(FeatureType.CATEGORY);
            item_feature.setWeight((short)1);
            compositeDoc.feature_list.add(item_feature);
        }
        if (one_json.get("source_id") != null) {
            String source_id = one_json.get("source_id").toString();
            media_doc.setSource(source_id);
        }
        if (one_json.get("tags") != null) {
            JSONArray json_array = JSONArray.fromObject(one_json.get("tags"));
            if (json_array != null) {
                for (int i = 0; i < json_array.size(); ++i) {
                    JSONObject one_tag = json_array.getJSONObject(i);
                    String name = one_tag.get("name").toString();;

                    // ADD it to feature list
                    ItemFeature item_feature = new ItemFeature();
                    item_feature.setName("TAG_" + name);
                    item_feature.setType(FeatureType.TAG);
                    item_feature.setWeight((short) 1);
                    compositeDoc.feature_list.add(item_feature);
                }
            }
        }
         List<ProductCode> pcodes = new ArrayList<ProductCode>();
       // pcodes.add(shared.datatypes.ProductCode.INDIA_IMAGETEXT);       //TODO
        compositeDoc.setPcodes(pcodes);
        compositeDoc.pcodes.add(shared.datatypes.ProductCode.INDIA_IMAGETEXT);
        compositeDoc.setMedia_doc_info(media_doc);
        return compositeDoc;
    }
    static public CompositeDoc FromJsonStringToCompositeDoc(String json_str) throws ParseException {
        JSONObject one_json;
        try {
            one_json = JSONObject.fromObject(json_str);
        } catch (Exception e) {
            e.printStackTrace();
            return  null;
        }
        //return FromJsonToCompositeDoc(one_json);
        CompositeDoc doc = null;
        try {
            doc = FromJsonToCompositeDoc(one_json);
        } catch (Exception e) {
            System.err.println("PARSE JSON ERROR:" + json_str);
            e.printStackTrace();
        }
        return doc;
        //ret_composite.setDescription(json_str);
    }

    public ArrayList<CompositeDoc> FromJsonToComposite(String  json_str) {
        JSONObject one_json = JSONObject.fromObject(json_str);
        JSONObject provider = JSONObject.fromObject(one_json.get("provider").toString());
        String source = provider.get("providerName").toString();
        JSONArray doc_list = JSONArray.fromObject(one_json.get("informations").toString());
        ArrayList<CompositeDoc> composite_list = new ArrayList<CompositeDoc>();
        //CompositeDoc composite_list = new Integer();
        for (int i = 0; i < doc_list.size(); i++) {
            CompositeDoc compositeDoc = new CompositeDoc();
            MediaDocInfo media_doc = new MediaDocInfo();
            media_doc.setSource(source);
            JSONObject one_doc = doc_list.getJSONObject(i);
            media_doc.setName(one_doc.get("title").toString());
            media_doc.setCategory_name(one_doc.get("category").toString());
            compositeDoc.setMedia_doc_info(media_doc);
            compositeDoc.setDescription(one_doc.get("abstract").toString());
            composite_list.add(compositeDoc);
        }
        return composite_list;
    }
//    public CompositeDoc DocumentConverter(String json_str){
//        JSONObject json = JSONObject.fromObject(json_str);
//        JSONObject provider = JSONObject.fromObject(json.get("provider").toString());
//        String source = provider.get("providerName").toString();
//        JSONArray doc_list = JSONArray.fromObject(json.get("informations").toString());
//        String res=null;
//        JSONObject jsonObject = JSONObject.fromObject(json_str);
//        CompositeDoc compositeDoc = new CompositeDoc();
//        StringBuilder sb = new StringBuilder();
//        for (int i = 0; i < doc_list.size(); i++) {
//            ArrayList<CompositeDoc> composite_list = new ArrayList<CompositeDoc>();
////            CompositeDoc compositeDoc = new CompositeDoc();
//            MediaDocInfo media_doc = new MediaDocInfo();
//            media_doc.setSource(source);
//            JSONObject one_doc = doc_list.getJSONObject(i);
//            media_doc.setName(one_doc.get("title").toString());
//            media_doc.setCategory_name(one_doc.get("category").toString());
//            compositeDoc.setMedia_doc_info(media_doc);
//            composite_list.add(compositeDoc);
//            sb.append(compositeDoc);
//        }
//        return compositeDoc;
//    }
    public static void main(String[] args) throws Exception{
        File file =  new File("E:\\Temp\\input.txt");
        BufferedReader br = new BufferedReader(new FileReader(file));
//        FileOutputStream out = new FileOutputStream("E:\\Temp\\docout.txt");
        CompositeDoc compositeDoc = new CompositeDoc();
        DocumentAdapter adapter = new DocumentAdapter();
        String line = null;
        ArrayList<CompositeDoc> compositeList= new ArrayList<CompositeDoc>();
        while((line=br.readLine())!=null){
//        System.out.println(adapter.DocumentConverter(line));
//            compositeDoc = adapter.DocumentConverter(line);
            compositeList=adapter.FromJsonToComposite(line);
        }
//        System.out.println(compositeDoc);
        Context context = null;
        String text = null;
        for(int i = 0; i < compositeList.size(); i++){
            text = CompositeDocSerialize.Serialize(compositeList.get(i),context);
            System.out.println(text);
            compositeDoc = CompositeDocSerialize.DeSerialize(text,context);
            System.out.println(compositeDoc);
        }
//        text = CompositeDocSerialize.Serialize(compositeDoc,context);
//        System.out.println(text);
//        compositeDoc = CompositeDocSerialize.DeSerialize(text,context);
//        System.out.println(compositeDoc.media_doc_info.source);
    }
}

