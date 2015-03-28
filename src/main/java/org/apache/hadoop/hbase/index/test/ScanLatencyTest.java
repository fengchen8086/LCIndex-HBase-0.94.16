package org.apache.hadoop.hbase.index.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.client.IndexConstants;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.index.client.RangeList;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

public class ScanLatencyTest {
  public static void main(String[] args) throws IOException {
    HTable table = new HTable(HBaseConfiguration.create(), "orders");

    double c3_end = 15000.0;
    String c5_equal = "1-URGENT";
    boolean useIndex = true;
    boolean print = false;
    int caching = 1000;
    int runTimes=5;
    long totalTime=0;

    for(int i=0;i<runTimes;i++){
      Scan scan = new Scan();
      FilterList filters = new FilterList();
      RangeList list = new RangeList();

      if (c5_equal != null) {
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c5"),
            CompareOp.EQUAL, Bytes.toBytes(c5_equal)));
        list.addRange(new Range(Bytes.toBytes("f:c5"), Bytes.toBytes(c5_equal), CompareOp.EQUAL,
            null, CompareOp.NO_OP));
      }

      if (c3_end > 0) {
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),
            CompareOp.LESS, Bytes.toBytes(c3_end)));
        list.addRange(new Range(Bytes.toBytes("f:c3"), null, CompareOp.NO_OP, Bytes.toBytes(c3_end),
            CompareOp.LESS));
      }

      scan.setFilter(filters);
      if (useIndex) {
//        scan.setAttribute(IndexConstants.SCAN_INDEX_RANGE, Writables.getBytes(list));
      }
      scan.setCacheBlocks(false);
      scan.setCaching(caching);
      
      long startTime = System.currentTimeMillis();
      ResultScanner scanner = table.getScanner(scan);
      Result[] result = scanner.next(caching);
      
      if (print) {
        for (Result r : result) {
          println(r);
        }
      }
      
      long stopTime = System.currentTimeMillis();
      System.out.println("Time elapsed: " + (stopTime - startTime) + " ms, result count: " + result.length);
      totalTime+=stopTime-startTime;
      scanner.close();
    }
    
    System.out.println("Final Time elapsed: " + (totalTime/runTimes) + " ms");

    table.close();
  }

  static void println(Result result) {
    StringBuilder sb = new StringBuilder();
    sb.append("row=" + Bytes.toString(result.getRow()));

    List<KeyValue> kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c1"));
    if (kv.size() != 0) {
      sb.append(", f:c1=" + Bytes.toInt(kv.get(0).getValue()));
    }

    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c2"));
    if (kv.size() != 0) {
      sb.append(", f:c2=" + Bytes.toString(kv.get(0).getValue()));
    }

    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c3"));
    if (kv.size() != 0) {
      sb.append(", f:c3=" + Bytes.toDouble(kv.get(0).getValue()));
    }

    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c4"));
    if (kv.size() != 0) {
      sb.append(", f:c4=" + Bytes.toString(kv.get(0).getValue()));
    }
    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c5"));
    if (kv.size() != 0) {
      sb.append(", f:c5=" + Bytes.toString(kv.get(0).getValue()));
    }

    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c6"));
    if (kv.size() != 0) {
      sb.append(", f:c6=" + Bytes.toString(kv.get(0).getValue()));
    }
    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c7"));
    if (kv.size() != 0) {
      sb.append(", f:c7=" + Bytes.toInt(kv.get(0).getValue()));
    }
    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c8"));
    if (kv.size() != 0) {
      sb.append(", f:c8=" + Bytes.toString(kv.get(0).getValue()));
    }
    System.out.println(sb.toString());
  }
}
