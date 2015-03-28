package org.apache.hadoop.hbase.index.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.IndexConstants;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanPreprocessTest {
  public static void main(String[] args) throws IOException {
    HTable table = new HTable(HBaseConfiguration.create(), "orders");

    Scan scan = new Scan();
    FilterList f1 = new FilterList(Operator.MUST_PASS_ALL);
    
    f1.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),
      CompareOp.LESS, Bytes.toBytes(3000.0)));
    
    f1.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c5"),
        CompareOp.EQUAL, Bytes.toBytes("1-URGENT")));
    
    scan.setFilter(f1);
    scan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Bytes.toBytes(true));
    scan.setAttribute(IndexConstants.MAX_SCAN_SCALE, Bytes.toBytes(0.5f));

    scan.setCacheBlocks(false);
    scan.setCaching(100000);
//    scan.setStopRow(Bytes.toBytes("13"));

    ResultScanner scanner=table.getScanner(scan);
    Result result = null;
    long startTime = System.currentTimeMillis();
    int count = 0;
    boolean print=false;
    while ((result = scanner.next()) != null) {
      count++;
      if (print) {
        println(result);
      }

      if (count % 100000 == 0) {
        System.out.println("Time elapsed: " + (System.currentTimeMillis() - startTime)
            + " ms, result count: " + count);
      }
    }

    long stopTime = System.currentTimeMillis();
    System.out.println("Time elapsed: " + (stopTime - startTime) + " ms, result count: " + count);
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
