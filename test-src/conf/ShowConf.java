package conf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class ShowConf {
  Configuration conf;

  public static void usage() {
    System.out.println("ShowConf configure-file-path assign-value-file");
  }

  public void work(String file1, String file2) throws IOException {
    conf = HBaseConfiguration.create();
    conf.addResource(file1);
    System.out.println("coffey param of flush size: "
        + conf.get("hbase.hregion.memstore.flush.size"));
    parseMannuallyAssignedFile(file2);
    System.out.println("coffey param of flush size: "
        + conf.get("hbase.hregion.memstore.flush.size"));
  }

  private void parseMannuallyAssignedFile(String file) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    String parts[] = null;
    while ((line = br.readLine()) != null) {
      parts = line.split(" ");
      if (parts.length != 3) {
        System.out.println("error");
      }
      if ("Long".equalsIgnoreCase(parts[1])) {
        conf.setLong(parts[0], Long.valueOf(parts[2]));
      } else if ("Boolean".equalsIgnoreCase(parts[1])) {
        conf.setBoolean(parts[0], Boolean.valueOf(parts[2]));
      } else if ("Int".equalsIgnoreCase(parts[1])) {
        conf.setInt(parts[0], Integer.valueOf(parts[2]));
      } else if ("String".equalsIgnoreCase(parts[1])) {
        conf.setStrings(parts[0], parts[2]);
      } else if ("Float".equalsIgnoreCase(parts[1])) {
        conf.setFloat(parts[0], Float.valueOf(parts[2]));
      }
    }
    br.close();
  }

  public static void main(String args[]) throws IOException {
    if (args.length != 2) {
      usage();
      return;
    }
    new ShowConf().work(args[0], args[1]);
  }
}
