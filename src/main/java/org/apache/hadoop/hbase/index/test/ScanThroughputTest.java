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

public class ScanThroughputTest {

  double c3_end = 100000.0;
  String c5_equal = "1-URGENT";
  boolean useIndex = false;
  boolean print = false;
  int caching = 1;

  int resultCount = 0;
  int threads = 30;
  int finishThreads = 0;
  long startTime = 0;

  public ScanThroughputTest() {
    byte[][] keys = new byte[][] { Bytes.toBytes("0000000000"), Bytes.toBytes("9999999999") };

    if (threads == 9) {
      keys = Bytes.split(Bytes.toBytes("0000000000"), Bytes.toBytes("9999999999"), threads - 1);
    }
    if (threads == 20) {
      keys =
          new byte[][] { Bytes.toBytes("0000000000"), Bytes.toBytes("05"), Bytes.toBytes("10"),
              Bytes.toBytes("15"), Bytes.toBytes("20"), Bytes.toBytes("25"), Bytes.toBytes("30"),
              Bytes.toBytes("35"), Bytes.toBytes("40"), Bytes.toBytes("45"), Bytes.toBytes("50"),
              Bytes.toBytes("55"), Bytes.toBytes("60"), Bytes.toBytes("65"), Bytes.toBytes("70"),
              Bytes.toBytes("75"), Bytes.toBytes("80"), Bytes.toBytes("85"), Bytes.toBytes("90"),
              Bytes.toBytes("95"), Bytes.toBytes("9999999999") };
    }

    if (threads == 30) {
      keys =
          new byte[][] { Bytes.toBytes("0000000000"), Bytes.toBytes("033"), Bytes.toBytes("066"),
              Bytes.toBytes("10"), Bytes.toBytes("13"), Bytes.toBytes("16"), Bytes.toBytes("20"),
              Bytes.toBytes("23"), Bytes.toBytes("26"), Bytes.toBytes("30"), Bytes.toBytes("33"),
              Bytes.toBytes("36"), Bytes.toBytes("40"), Bytes.toBytes("43"), Bytes.toBytes("46"),
              Bytes.toBytes("50"), Bytes.toBytes("53"), Bytes.toBytes("56"), Bytes.toBytes("60"),
              Bytes.toBytes("63"), Bytes.toBytes("66"), Bytes.toBytes("70"), Bytes.toBytes("73"),
              Bytes.toBytes("76"), Bytes.toBytes("80"), Bytes.toBytes("83"), Bytes.toBytes("86"),
              Bytes.toBytes("90"), Bytes.toBytes("93"), Bytes.toBytes("96"),
              Bytes.toBytes("9999999999") };
    }

    if (threads == 50) {
      keys =
          new byte[][] { Bytes.toBytes("0000000000"), Bytes.toBytes("02"), Bytes.toBytes("04"),
              Bytes.toBytes("06"), Bytes.toBytes("08"), Bytes.toBytes("10"), Bytes.toBytes("12"),
              Bytes.toBytes("14"), Bytes.toBytes("16"), Bytes.toBytes("18"), Bytes.toBytes("20"),
              Bytes.toBytes("22"), Bytes.toBytes("24"), Bytes.toBytes("26"), Bytes.toBytes("28"),
              Bytes.toBytes("30"), Bytes.toBytes("32"), Bytes.toBytes("34"), Bytes.toBytes("36"),
              Bytes.toBytes("38"), Bytes.toBytes("40"), Bytes.toBytes("42"), Bytes.toBytes("44"),
              Bytes.toBytes("46"), Bytes.toBytes("48"), Bytes.toBytes("50"), Bytes.toBytes("52"),
              Bytes.toBytes("54"), Bytes.toBytes("56"), Bytes.toBytes("58"), Bytes.toBytes("60"),
              Bytes.toBytes("62"), Bytes.toBytes("64"), Bytes.toBytes("66"), Bytes.toBytes("68"),
              Bytes.toBytes("70"), Bytes.toBytes("72"), Bytes.toBytes("74"), Bytes.toBytes("76"),
              Bytes.toBytes("78"), Bytes.toBytes("80"), Bytes.toBytes("82"), Bytes.toBytes("84"),
              Bytes.toBytes("86"), Bytes.toBytes("88"), Bytes.toBytes("90"), Bytes.toBytes("92"),
              Bytes.toBytes("94"), Bytes.toBytes("96"), Bytes.toBytes("98"),
              Bytes.toBytes("9999999999") };
    }

    for (byte[] key : keys) {
      System.out.println(Bytes.toStringBinary(key));
    }

    ScanThread[] scans = new ScanThread[threads];
    for (int i = 0; i < threads; i++) {
      scans[i] = new ScanThread(keys[i], keys[i + 1]);
      scans[i].setName("T-" + i);
    }

    for (int i = 0; i < threads; i++) {
      scans[i].start();
    }

    startTime = System.currentTimeMillis();

    for (int i = 0; i < threads; i++) {
      try {
        scans[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  double maxThroughput = 0;

  public synchronized void addResults(int num, boolean finish) {
    resultCount += num;
    if (finish) finishThreads++;

    long time = System.currentTimeMillis();
    double throughput = resultCount * 1000.0 / (time - startTime);
    if (throughput > maxThroughput) maxThroughput = throughput;
    System.out.println("throughput=" + throughput + ", alive=" + (threads - finishThreads)
        + ", result=" + resultCount);
    if (threads == finishThreads) System.out.println(maxThroughput);
  }

  public static void main(String[] args) throws IOException {
    new ScanThroughputTest();
  }

  class ScanThread extends Thread {
    byte[] start = null;
    byte[] stop = null;

    public ScanThread(byte[] start, byte[] stop) {
      this.start = start;
      this.stop = stop;
    }

    public void run() {
      HTable table = null;
      try {
        table = new HTable(HBaseConfiguration.create(), "orders");

        Scan scan = new Scan();
        scan.setStartRow(start);
        scan.setStopRow(stop);
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
          list.addRange(new Range(Bytes.toBytes("f:c3"), null, CompareOp.NO_OP, Bytes
              .toBytes(c3_end), CompareOp.LESS));
        }

        scan.setFilter(filters);
        if (useIndex) {
//          scan.setAttribute(IndexConstants.SCAN_INDEX_RANGE, Writables.getBytes(list));
        }
        scan.setCacheBlocks(false);
        scan.setCaching(caching);

        long startTime = System.currentTimeMillis();
        ResultScanner scanner = table.getScanner(scan);
        int count = 0;
        Result[] result = null;

        while ((result = scanner.next(caching > 10000 ? caching : 10000)) != null
            && result.length != 0) {
          addResults(result.length, false);
          count += result.length;
          if (print) {
            for (Result r : result) {
              println(r);
            }
          }
        }
        addResults(0, true);

        long stopTime = System.currentTimeMillis();
        System.out.println(this.getName() + " throughput:" + count * 1000.0
            / (stopTime - startTime));

        scanner.close();

        table.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
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
