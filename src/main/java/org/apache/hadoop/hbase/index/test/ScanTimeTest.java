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

public class ScanTimeTest {
  public static void main(String[] args) throws IOException {
    HTable table = new HTable(HBaseConfiguration.create(), "orders");

    double[] c3_value = 
//        new double[] {1500.0, 3000.0, 7000.0, 15000.0, 25000.0, 50000.0, 60000.0, 70000.0,
//        90000.0,120000.0,150000.0,170000.0, 200000.0, 230000.0, 250000.0,500000.0};
        
//     new double[] {1000.0, 1500.0, 2000.0, 3000.0, 5000.0, 7000.0, 10000.0, 13000.0, 15000.0,
//     20000.0, 25000.0, 30000.0, 35000.0, 40000.0, 45000.0, 50000.0, 60000.0, 70000.0,
//     80000.0,90000.0,120000.0,150000.0,170000.0};
        
        new double[] {90000.0};
    
//    String[] c5_value =new String[]{"1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW"};
    for (double v : c3_value) {
      System.out.println(v);
      double c3_end = v;
      String c5_equal = "1-URGENT";
      // "2-HIGH";
      boolean useIndex = true;
      boolean printProgress = false;
      boolean print = false;

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
        list.addRange(new Range(Bytes.toBytes("f:c3"), null, CompareOp.NO_OP,
          Bytes.toBytes(c3_end), CompareOp.LESS));
      } 
      
      scan.setFilter(filters);
      if (useIndex) {
//        scan.setAttribute(IndexConstants.SCAN_INDEX_RANGE, Writables.getBytes(list));
      }
      scan.setCacheBlocks(false);
      scan.setCaching(100000);

      long startTime = System.currentTimeMillis();
      int count = 0;
      ResultScanner scanner = table.getScanner(scan);
      Result result = null;

      while ((result = scanner.next()) != null) {
        count++;
        if (print) {
          println(result);
        }

        if (printProgress && (count % 100000 == 0)) {
          System.out.println("Time elapsed: " + (System.currentTimeMillis() - startTime)
              + " ms, result count: " + count);
        }
      }

      long stopTime = System.currentTimeMillis();
      System.out.println("Time elapsed: " + (stopTime - startTime) + " ms, result count: " + count);
    }

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
