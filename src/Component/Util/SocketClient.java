package Component.Util;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.KeyValue;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by sunhaochuan on 2017/3/1.
 */
public class SocketClient {
    public List<Map.Entry<String, List<String>>> OutBandProcessBySocket(ArrayList<Map.Entry<String, String> > input) {
        List<Map.Entry<String, List<String>>> res = new ArrayList<Map.Entry<String, List<String>>>();

        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (int i = 0; i < input.size(); ++i) {
            sb.append(Base64.encodeBase64(input.get(i).getValue().getBytes()));
            sb.append('\t');
            count ++;
            if (count == 100) {
                String labels = SocketConnect(sb.toString());

                count = 0;
                sb = new StringBuilder();
            }
        }
        if (count != 0) {
            String labels = SocketConnect(sb.toString());
        }

        return  res;
    }

    public String SocketConnect(String text) {
        String content = "";
        try {
            //客户端
            //1、创建客户端Socket，指定服务器地址和端口
            Socket socket =new Socket("10.11.145.35", 51423);
            //2、获取输出流，向服务器端发送信息
            OutputStream os = socket.getOutputStream();//字节输出流
            PrintWriter pw =new PrintWriter(os);//将输出流包装成打印流
            pw.write(text);
            pw.flush();
            socket.shutdownOutput();
            //3、获取输入流，并读取服务器端的响应信息
            InputStream is = socket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String info = null;
            char[] buffer = new char [10240];
            Integer read_len = 0;
            while((read_len = br.read(buffer, 0, 10240))!= -1){
                content = content + new String(buffer, 0, read_len);
            }

            //4、关闭资源
            br.close();
            is.close();
            pw.close();
            os.close();
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }

    public static void main(String[] args) {
        SocketClient s = new SocketClient();
        System.out.println(s.SocketConnect("http://www.toutiao.com/a6387604097701150977/"));
    }
}
