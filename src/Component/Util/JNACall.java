package Component.Util;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;

import java.io.*;

/**
 * Created by sunhaochuan on 2017/3/3.
 */
public class JNACall {

    public interface CLibrary extends Library {

        String abc = Init();
        CLibrary INSTANCE = (CLibrary)Native.loadLibrary("fasttext", CLibrary.class);
        boolean init_model = false;

        void LoadModel(String file_path, int idx);
        String Predict(String input_text, int k, int idx);
    }

    public static void RunSystemCommand(String[] command, File file) {
        if (command != null && !command.equals("")) {
            try {
                Process ps = null;
                if (file != null)
                    ps = Runtime.getRuntime().exec(command, null, file);
                else
                    ps = Runtime.getRuntime().exec(command);
                String message = loadStream(ps.getInputStream());
                String errorMeg = loadStream(ps.getErrorStream());
                System.out.println(message);
                System.out.println("-------");
                System.out.println(errorMeg);
                try {
                    ps.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static String Init() {
        String[] commonds = {"sh", "-c", "export LD_LIBRARY_PATH=./;echo $LD_LIBRARY_PATH"};
        RunSystemCommand(commonds, null);
        System.out.println("Start to load the libary!");

        return "";
    }


    private static String loadStream(InputStream in) throws IOException {
        int ptr = 0;
        in = new BufferedInputStream(in);
        StringBuffer buffer = new StringBuffer();
        while ((ptr = in.read()) != -1) {
            buffer.append((char) ptr);
        }
        return new String(buffer.toString().getBytes("ISO-8859-1"), "GBK");
    }

    public static void main(String[] args) {
        CLibrary.INSTANCE.LoadModel("model.bin", 0);

        String fileName = "sample.txt";
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一行");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            //一次读一行，读入null时文件结束
            while ((tempString = reader.readLine()) != null) {
                //把当前行号显示出来
                String res =  CLibrary.INSTANCE.Predict(tempString, 10, 0);
                System.out.println("case: " + line + ": " + res);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
}
