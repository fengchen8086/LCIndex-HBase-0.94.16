package doTest.scan;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.index.client.DataType;

import doTest.put.PutTestConstants;
import doTest.put.PutTestConstants.CF_INFO;

public abstract class ClassScanBase {

  class CheckedInfo {
    boolean isFull = false;
    Map<String, Boolean> hasValue = new TreeMap<String, Boolean>();

    public CheckedInfo() {
      List<CF_INFO> cfs = PutTestConstants.getCFInfo();
      for (CF_INFO ci : cfs) {
        hasValue.put(ci.qualifier, false);
      }
    }

    public void checkFull() {
      boolean supposeFull = true;
      for (Entry<String, Boolean> entry : hasValue.entrySet()) {
        if (entry.getValue() == false) {
          supposeFull = false;
          break;
        }
      }
      isFull = supposeFull;
    }

    public String stringForCheck() {
      if (isFull) return "ERROR, this is full!";
      StringBuilder sb = new StringBuilder();
      for (Entry<String, Boolean> entry : hasValue.entrySet()) {
        if (entry.getValue() == false) {
          sb.append("qualifier " + entry.getKey() + " is empty. ");
        }
      }
      return sb.toString();
    }
  }

  final String tableName;
  final Configuration conf;
  final int startValue = 0;
  final int stopValue;
  final HBaseAdmin admin;
  final double percentage;
  final String FAMILY_NAME = PutTestConstants.FAMILY_NAME;
  final int RECORD_NUMBER;
  final int PRINT_INTERVAL;
  final int PRINT_TIME = 10;
  final String suffix = ".dat";
  String outputPath;
  HTable table;
  Map<String, CheckedInfo> mapForCheck;
  List<CF_INFO> cfs = PutTestConstants.getCFInfo();
  Writer writer = null;

  public ClassScanBase(String confPath, String newAddedFile, String tableName, int recordNumber,
      double percentage, String outputPath) throws IOException {
    this.tableName = tableName;
    this.percentage = percentage;
    this.RECORD_NUMBER = recordNumber;
    conf = HBaseConfiguration.create();
    conf.addResource(confPath);
    admin = new HBaseAdmin(conf);
    PRINT_INTERVAL = RECORD_NUMBER / PRINT_TIME;
    PutTestConstants.parseMannuallyAssignedFile(conf, newAddedFile);
    if (outputPath != null) {
      this.outputPath = outputPath + suffix;
      writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.outputPath)));
    }
    table = new HTable(conf, tableName);
    mapForCheck = new TreeMap<String, CheckedInfo>();
    List<CF_INFO> cfs = PutTestConstants.getCFInfo();
    int counter = 0, max = -1;
    for (CF_INFO ci : cfs) {
      if (ci.isIndex) {
        ++counter;
        if (ci.scale > max) max = ci.scale;
      }
    }
    if (counter == 2) {
      stopValue = (int) (RECORD_NUMBER * Math.sqrt(percentage));
    } else if (counter == 3) {
      if (percentage >= 1.0) {
        stopValue = max * RECORD_NUMBER;
      } else if (percentage >= 0.25) { // 0.5 when 1/2/4, 0.25 when 1/4/16
        stopValue = (int) (percentage * max * recordNumber);
      } else if (percentage >= (1.0 / 64)) { // 0.125 when 1/2/4, 1.0 / 64 when 1/4/16
        // stopValue = (int) (RECORD_NUMBER * Math.pow(percentage / 2.0, 1.0 / 2) * 2);
        stopValue = (int) (RECORD_NUMBER * Math.pow(percentage, 1.0 / 2) * 8); // for 1/4/16
      } else {
        stopValue = (int) (RECORD_NUMBER * Math.pow(percentage, 1.0 / 3) * 4);
      }
    } else {
      stopValue = -1; // not support
    }
    if (startValue > stopValue) {
      throw new IOException("error percentage " + percentage + " leads to error stopvalue: "
          + stopValue);
    }
  }

  protected DataType getType(String str) {
    for (CF_INFO ci : cfs) {
      if (ci.qualifier.equalsIgnoreCase(str)) {
        return ci.type;
      }
    }
    return DataType.STRING;
  }

  public abstract ResultScanner getScanner() throws IOException;

  public abstract void printString(Result result);

  public void finish() {
    try {
      if (table != null) {
        table.close();
      }
      if (writer != null) {
        System.out.println("coffey closing file: " + outputPath);
        writer.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public abstract boolean insertResultForCheck(Result result) throws IOException;

  public int finalCheck() {
    int checkedCount = 0;
    for (Entry<String, CheckedInfo> entry : mapForCheck.entrySet()) {
      if (!entry.getValue().isFull) {
        System.out.println("key " + entry.getKey() + " is not full, value: "
            + entry.getValue().stringForCheck());
        return -1;
      }
      ++checkedCount;
    }
    return checkedCount;
  }
}
