package Component.Util;

/**
 * Created by sunhaochuan on 2017/4/6.
 */



import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TTransportException;

public class ThriftClient {

    public void startClient() {
        TTransport transport;
        try {
            transport = new TSocket("10.121.145.29", 9091);
            TProtocol protocol = new TBinaryProtocol(transport);
            transport.open();
            FastText.Client client = new FastText.Client(protocol);
            System.out.println(client.say("应对雄安炒房客可疏堵结合 法制晚报今日快评新闻提示自中央决定设立河北雄安新区消息发布后"));
            transport.close();
        } catch (TTransportException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ThriftClient client = new ThriftClient();
        client.startClient();
    }
}