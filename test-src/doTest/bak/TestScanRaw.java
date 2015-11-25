package doTest.bak;

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

public class TestScanRaw {
  public static void main(String[] args) throws IOException {
    double[] c3_value = new double[] { 50 };
    for (double v : c3_value) {
      System.out.println(v);
      double c3_end = v;
      boolean useIndex = true;
      boolean printProgress = false;
      boolean print = true;

      Scan scan = new Scan();
      FilterList filters = new FilterList();
      RangeList list = new RangeList();

      if (c3_end > 0) {
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),
            CompareOp.LESS, Bytes.toBytes(c3_end)));
        list.addRange(new Range(Bytes.toBytes("f:c3"), null, CompareOp.NO_OP,
            Bytes.toBytes(c3_end), CompareOp.LESS));
      }
      

      scan.setFilter(filters);

      if (useIndex) {
        scan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Writables.getBytes(list));
        scan.setAttribute(IndexConstants.MAX_SCAN_SCALE, Bytes.toBytes(1.0));
      }

      for (Range r : list.getRangeList()) {
        System.out.println(r);
      }
     
      scan.setCacheBlocks(false);
      scan.setCaching(30);

      long startTime = System.currentTimeMillis();
      int count = 0;

      HTable table = new HTable(HBaseConfiguration.create(), "base");
      ResultScanner scanner = table.getScanner(scan);
      Result result = null;

      while ((result = scanner.next()) != null) {
        count++;
        if (print) {
          println(result);
        }

        if (printProgress && (count % 100 == 0)) {
          System.out.println("Time elapsed: " + (System.currentTimeMillis() - startTime)
              + " ms, result count: " + count);
        }
      }

      long stopTime = System.currentTimeMillis();
      System.out.println("Time elapsed: " + (stopTime - startTime) + " ms, result count: " + count);
      table.close();
    }

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
      // sb.append(", f:c3=" + Bytes.toString(kv.get(0).getValue()));
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
