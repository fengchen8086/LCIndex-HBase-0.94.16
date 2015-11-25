package doTest.old.lcc;

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
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.index.client.RangeList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import doWork.LCCIndexConstant;
import doWork.LCCIndexGenerator;

public class LCCScanRaw {
  public static void main(String[] args) throws IOException {
    double start_double = 10;
    double end_double = 100;
    int start_int = 900;
    int end_int = 999;
    System.out.println("scan A range: (" + start_double + ", " + end_double + ")");
    System.out.println("scan B range: (" + start_int + ", " + end_int + ")");
    boolean useIndex = true;
    boolean printProgress = false;
    boolean print = true;

    Scan scan = new Scan();
    FilterList filters = new FilterList();
    RangeList list = new RangeList();

    if (end_double > 0) {
      filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("A"),
          CompareOp.LESS, Bytes.toBytes(end_double)));
      // int first!
      list.addRange(new Range(Bytes.toBytes("f:A"), Bytes.toBytes(start_int),
          CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(end_int), CompareOp.LESS));
      list.addRange(new Range(Bytes.toBytes("f:B"), Bytes.toBytes(start_double),
          CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(end_double), CompareOp.LESS_OR_EQUAL));
    }

    scan.setFilter(filters);

    if (useIndex) {
      scan.setAttribute(LCCIndexConstant.SCAN_WITH_LCCINDEX, Writables.getBytes(list));
    }
    // scan.setAttribute(LCCIndexConstant.TESTING_SIGNAL, Bytes.toBytes("yes"));

    for (Range r : list.getRangeList()) {
      System.out.println(r);
    }

    scan.setCacheBlocks(false);
    scan.setCaching(30);

    long startTime = System.currentTimeMillis();
    int count = 0;

    // HTable table =
    // new HTable(HBaseConfiguration.create(), LCCIndexConstant.TEST_IRINDEX_TABLE_NAME);

    HTable table =
        new HTable(HBaseConfiguration.create(), LCCIndexConstant.TEST_LCCINDEX_TABLE_NAME);
    ResultScanner scanner = table.getScanner(scan);
    Result result = null;
    System.out.println(scanner.getClass().getName());
    while ((result = scanner.next()) != null) {
      count++;
      if (print) {
        System.out.println("got " + result.size() + " rows");
        if (useIndex) {
          tempPrintln(result);
        } else {
          println(result);
        }
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

  static void tempPrintln(Result result) {
    for (KeyValue kv : result.list()) {
      System.out.println(LCCIndexConstant.mWinterToPrint(kv));
    }
  }

  static void println(Result result) {
    StringBuilder sb = new StringBuilder();
    System.out.println("got " + result.size() + " rows");
    sb.append("row=" + Bytes.toString(result.getRow()));

    List<KeyValue> kv = null;
    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("A"));
    if (kv.size() != 0) {
      sb.append(", f:A=" + Bytes.toDouble(kv.get(0).getValue()));
    }
    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("B"));
    if (kv.size() != 0) {
      sb.append(", f:B=" + Bytes.toInt(kv.get(0).getValue()));
    }
    kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("Info"));
    if (kv.size() != 0) {
      sb.append(", f:Info=" + Bytes.toString(kv.get(0).getValue()));
    }
    System.out.println(sb.toString());
  }
}
