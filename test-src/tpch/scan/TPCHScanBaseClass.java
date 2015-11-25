package tpch.scan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.index.client.Range;

import tpch.put.TPCHConstants;

public abstract class TPCHScanBaseClass {

  final String tableName;
  final Configuration conf;
  final HBaseAdmin admin;
  HTable table;
  final List<Range> ranges;

  public TPCHScanBaseClass(String confPath, String newAddedFile, String tableName,
      List<Range> ranges) throws IOException {
    this.tableName = tableName;
    conf = HBaseConfiguration.create();
    conf.addResource(confPath);
    TPCHConstants.parseMannuallyAssignedFile(conf, newAddedFile);
    System.out.println("coffey manually set zk " + "hbase.zookeeper.quorum "
        + " hec-14,hec-02,hec-03");
//    conf.set("hbase.zookeeper.quorum", "hec-14,hec-02,hec-03");
    admin = new HBaseAdmin(conf);
    table = new HTable(conf, tableName);
    this.ranges = ranges;
  }

  // protected DataType getType(String str) {
  // for (CF_INFO ci : cfs) {
  // if (ci.qualifier.equalsIgnoreCase(str)) {
  // return ci.type;
  // }
  // }
  // return DataType.STRING;
  // }

  public abstract ResultScanner getScanner() throws IOException;

  public abstract void printString(Result result);

  public void finish() {
    try {
      if (table != null) {
        table.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected Range selectTheBestRange() {
    int equal_id = -1, not_equal_id = -1, rangeID = -1;
    for (int i = 0; i < ranges.size(); ++i) {
      Range r = ranges.get(i);
      if (r.getStopValue() != null) {
        if (r.getStartValue() != null) return r;
        if (r.getStopType() == CompareOp.LESS || r.getStopType() == CompareOp.LESS_OR_EQUAL) {
          rangeID = i;
        } else if (r.getStopType() == CompareOp.EQUAL) {
          equal_id = i;
        } else if (r.getStopType() == CompareOp.NOT_EQUAL) {
          not_equal_id = i;
        }
      } else {
        if (r.getStartType() == CompareOp.GREATER || r.getStartType() == CompareOp.GREATER_OR_EQUAL) {
          rangeID = i;
        } else if (r.getStartType() == CompareOp.EQUAL) {
          equal_id = i;
        } else if (r.getStartType() == CompareOp.NOT_EQUAL) {
          not_equal_id = i;
        }
      }
    }
    if (rangeID != -1) return ranges.get(rangeID);
    if (equal_id != -1) return ranges.get(equal_id);
    if (not_equal_id != -1) return ranges.get(not_equal_id);
    return ranges.size() == 0 ? null : ranges.get(0);
  }
}
