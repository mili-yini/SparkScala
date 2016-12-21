package CompositeDocProcess;

import DocProcess.CompositeDocSerialize;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.hadoop.io.Text;
import pipeline.CompositeDoc;
import serving.mediadocinfo.MediaDocInfo;

import javax.naming.Context;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.ArrayList;

/**
 * Created by zhanglin5 on 2016/12/21.
 */
public class DocumentAdapter {
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

