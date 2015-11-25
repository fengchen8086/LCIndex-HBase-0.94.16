package conf;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseConfig {
  private static final Log LOG = LogFactory.getLog(HbaseConfig.class);
  private static final String HBASE_CONF_DIR = "/home/winter/softwares/hbase-0.94.16/conf";

  public static Configuration getHHConfig() throws IOException {
    Configuration conf = HBaseConfiguration.create();
//    PutTestConstants.parseMannuallyAssignedFile(conf, HBASE_CONF_DIR + "/winter-assign");
    return conf;
  }

  public static void main(String[] args) throws IOException {
    Configuration config = HbaseConfig.getHHConfig();
//    HTable table = new HTable(config, "lcc");
//    System.out.println(table.getTableName());
  }
}