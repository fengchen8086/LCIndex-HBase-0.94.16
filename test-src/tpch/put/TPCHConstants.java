package tpch.put;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.util.Bytes;

import doWork.LCCIndexConstant;

public class TPCHConstants {
  public static final String FILE_NAME_FORMAT = "%06d";

  public static String getDataFileName(String baseDir, int id) {
    return baseDir + "/" + String.format(FILE_NAME_FORMAT, id);
  }

  public static String getBaseDir(String baseDir, int maxRecord, int clientNumber,
      int eachThreadNumber) {
    return baseDir + "/" + maxRecord + "-" + clientNumber + "-" + eachThreadNumber;
  }

  public static String getClientDir(String baseDir, String hostName) {
    return baseDir + "/" + hostName;
  }

  public static final String FAMILY_NAME = "f";

  public static final String HBASE_TABLE_NAME = "tpch_raw";
  public static final String IR_TABLE_NAME = "tpch_ir";
  public static final String LCC_TABLE_NAME = "tpch_lcc";
  public static final String CCIndex_TABLE_NAME = "tpch_cc";
  public static final String CMIndex_TABLE_NAME = "tpch_cm";

  public static final int GENERATED_STRING_LENGTH = 270;

  public static final long ROUND_SLEEP_TIME = 10 * 1000;

  // client and server communication
  public static final int REMOTE_SERVER_PORT = 18756;
  public static final int REMOTE_FILE_TRANS_BUFFER_LEN = 500 * 1000; // 500k

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

  public static class TPCH_CF_INFO {
    public String qualifier;
    public DataType type;
    public boolean isIndex;
    public String startString;
    public String stopString;

    public TPCH_CF_INFO(String s, DataType type, boolean isIndex, String startString,
        String stopString) {
      qualifier = s;
      this.type = type;
      this.isIndex = isIndex;
    }

    public TPCH_CF_INFO(String s, DataType type, boolean isIndex) {
      this(s, type, isIndex, null, null);
    }
  }

  private static ArrayList<TPCH_CF_INFO> cfs = new ArrayList<TPCH_CF_INFO>();

  static {
    cfs.add(new TPCH_CF_INFO("ok", DataType.LONG, false));
    cfs.add(new TPCH_CF_INFO("ck", DataType.LONG, false));
    cfs.add(new TPCH_CF_INFO("st", DataType.STRING, false));
    cfs.add(new TPCH_CF_INFO("t", DataType.DOUBLE, true));
    cfs.add(new TPCH_CF_INFO("d", DataType.LONG, true));
    cfs.add(new TPCH_CF_INFO("p", DataType.STRING, true));
    cfs.add(new TPCH_CF_INFO("cl", DataType.STRING, false));
    cfs.add(new TPCH_CF_INFO("so", DataType.INT, false));
    cfs.add(new TPCH_CF_INFO("cm", DataType.STRING, false));
  }

  public static List<TPCH_CF_INFO> getCFInfo() {
    return cfs;
  }

  public static String printRange(Range r) {
    StringBuilder sb = new StringBuilder();
    sb.append("[" + Bytes.toString(r.getFamily()) + ":" + Bytes.toString(r.getQualifier())
        + "], values (");
    if (r.getStartValue() != null) {
      sb.append(LCCIndexConstant.getStringOfValueAndType(r.getDataType(), r.getStartValue()));
      if (r.getStartType() == CompareOp.EQUAL || r.getStartType() == CompareOp.NOT_EQUAL) {
        sb.append(" <== ").append(r.getStartType()).append(" )");
        return sb.toString();
      }
    } else {
      sb.append("null");
    }
    sb.append(", ");
    if (r.getStopValue() != null) {
      sb.append(LCCIndexConstant.getStringOfValueAndType(r.getDataType(), r.getStopValue()));
    } else {
      sb.append("MAX");
    }
    sb.append(")");
    return sb.toString();
  }
}
