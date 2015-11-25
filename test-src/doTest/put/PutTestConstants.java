package doTest.put;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.index.client.DataType;

public class PutTestConstants {

  public static final String FAMILY_NAME = "f";

  public static final String HBASE_TABLE_NAME = "raw";
  public static final String IR_TABLE_NAME = "ir";
  public static final String LCC_TABLE_NAME = "lcc";
  public static final String CCIndex_TABLE_NAME = "cc";
  public static final String CMIndex_TABLE_NAME = "cm";

  public static final int GENERATED_STRING_LENGTH = 270;

  public static final long ROUND_SLEEP_TIME = 10 * 1000;

  public static class CF_INFO {
    public String qualifier;
    public DataType type;
    public boolean isIndex;
    public int scale;

    public CF_INFO(String s, DataType type, boolean isIndex, int scale) {
      qualifier = s;
      this.type = type;
      this.isIndex = isIndex;
      this.scale = scale;
    }
  }

  private static ArrayList<CF_INFO> cfs = new ArrayList<CF_INFO>();

  static {
    cfs.add(new CF_INFO("A", DataType.DOUBLE, true, 16)); // 4
    cfs.add(new CF_INFO("B", DataType.INT, true, 4)); // 2
    cfs.add(new CF_INFO("C", DataType.INT, true, 1)); // 1
    cfs.add(new CF_INFO("Info", DataType.STRING, false, 1));
  }

  public static List<CF_INFO> getCFInfo() {
    return cfs;
  }

  public static void parseMannuallyAssignedFile(Configuration conf, String file) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    String parts[] = null;
    while ((line = br.readLine()) != null) {
      if (line.startsWith("#")) {
        System.out.println("skip line: " + line);
        continue;
      }
      parts = line.split("\t");
      if (parts.length != 2) {
        System.out.println("line error: " + line);
        continue;
      }
      System.out.println("setting line: " + line);
      conf.set(parts[0], parts[1]);
    }
    br.close();
  }
}
