package doTest.ir;

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

import doWork.LCCIndexConstant;

public class IRScan {
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
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("A"),
            CompareOp.LESS, Bytes.toBytes(c3_end)));
        list.addRange(new Range(Bytes.toBytes("f:A"), null, CompareOp.NO_OP, Bytes.toBytes(c3_end),
            CompareOp.LESS));
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

      HTable table = new HTable(HBaseConfiguration.create(), LCCIndexConstant.TEST_IRINDEX_TABLE_NAME);
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

    List<KeyValue> kv = null;
    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("A"));
    if (kv.size() != 0) {
      sb.append(", f:A=" + Bytes.toDouble(kv.get(0).getValue()));
    }
    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("B"));
    if (kv.size() != 0) {
      sb.append(", f:B=" + Bytes.toString(kv.get(0).getValue()));
    }
    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("Info"));
    if (kv.size() != 0) {
      sb.append(", f:Info=" + Bytes.toString(kv.get(0).getValue()));
    }
    System.out.println(sb.toString());
  }
}
