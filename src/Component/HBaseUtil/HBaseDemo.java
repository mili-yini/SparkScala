package Component.HBaseUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Created by sunhaochuan on 2017/4/6.
 */
public class HBaseDemo {


        //private static final Log LOG = LogFactory.getLog(RegionserverCheck.class.getName());
        public static void main(String [] args) throws IOException {

            Configuration hconf = HBaseConfiguration.create();
            try {
                hconf.addResource(new FileInputStream(new File("C:\\Users\\sunhaochuan\\Desktop\\hbase-site.xml")));
            } catch (Exception e) {
               // LOG.error("configuration file not found");
            }

            org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(hconf);
            HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();

            ClusterStatus hcluster = admin.getClusterStatus();
            admin.close();

            Collection<ServerName> deadRSs = hcluster.getDeadServerNames(); // get the dead regionserver
            int liveRegionServers = hcluster.getServersSize(); // get the live regionserver's number
            System.out.println(liveRegionServers);
            for(ServerName s : deadRSs){
                System.out.println(s.getHostname());
            }
        }

}
