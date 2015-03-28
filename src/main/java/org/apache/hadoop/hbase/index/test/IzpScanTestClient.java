package org.apache.hadoop.hbase.index.test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.IndexConstants;
import org.apache.hadoop.hbase.util.Bytes;

public class IzpScanTestClient {
  public static void main(String[] args) throws Throwable {
    Configuration conf = HBaseConfiguration.create();
    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
      100, 600, 10, TimeUnit.SECONDS,
      new LinkedBlockingQueue<Runnable>(),
      new ThreadPoolExecutor.CallerRunsPolicy());
    
    HTable table = new HTable(conf, Bytes.toBytes("izp30e"),threadPool);
    int testType = 2;
    boolean print = false;
    final String time[] = { "2014060100:00:00","2014063000:00:00"};

//    final String time[] = { "201406" ,"2014060100:00:00"};
    
    final Scan scan = new Scan();
//    scan.setStartRow(Bytes.toBytes("000"));
//    scan.setStopRow(Bytes.toBytes("001"));

    scan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Bytes.toBytes(true));
    scan.setAttribute(IndexConstants.MAX_SCAN_SCALE, Bytes.toBytes(0.3f));
    scan.setCacheBlocks(false);
    scan.setCaching(100);
    switch (testType) {
    case 1:
    {
      FilterList f1 = new FilterList(Operator.MUST_PASS_ALL);

      FilterList f2 = new FilterList(Operator.MUST_PASS_ONE);
      f2.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("h"),
          CompareOp.EQUAL, Bytes.toBytes("www.abcd.com")));
      f2.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("h"),
          CompareOp.EQUAL, Bytes.toBytes("www.efgh.com")));
      f2.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("h"),
          CompareOp.EQUAL, Bytes.toBytes("www.ijkl.com")));
      f2.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("h"),
        CompareOp.EQUAL, Bytes.toBytes("www.mnop.com")));
      f2.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("h"),
        CompareOp.EQUAL, Bytes.toBytes("www.qrst.com")));
      f1.addFilter(f2);

      f1.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("r"),
          CompareOp.LESS_OR_EQUAL, Bytes.toBytes("000000000fffffff")));

      f1.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("y"),
          CompareOp.EQUAL, Bytes.toBytes("C8")));

      FilterList f3 = new FilterList(Operator.MUST_PASS_ONE);
      f3.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("s"),
          CompareOp.EQUAL, Bytes.toBytes(7864560)));
      f3.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("s"),
          CompareOp.EQUAL, Bytes.toBytes(8192125)));
      f3.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("s"),
          CompareOp.EQUAL, Bytes.toBytes(10485840)));
      f1.addFilter(f3);

      scan.setFilter(f1);
      
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("h"));
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("r"));
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("y"));
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("s"));
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("w"));
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"));
    }
    break;
    case 2:
    {
      FilterList f1 = new FilterList(Operator.MUST_PASS_ALL);

      f1.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("o"),
          CompareOp.LESS_OR_EQUAL, Bytes.toBytes("000000ff")));
      f1.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("r"),
        CompareOp.LESS_OR_EQUAL, Bytes.toBytes("0000000000ffffff")));

      scan.setFilter(f1);
      
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("o"));
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("r"));
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("w"));
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"));
    }
    break;
    case 3:
    {
      FilterList f2 = new FilterList(Operator.MUST_PASS_ONE);
      f2.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("h"),
          CompareOp.EQUAL, Bytes.toBytes("www.abcd.com")));
      f2.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("h"),
          CompareOp.EQUAL, Bytes.toBytes("www.efgh.com")));
      f2.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("h"),
          CompareOp.EQUAL, Bytes.toBytes("www.ijkl.com")));
      f2.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("h"),
        CompareOp.EQUAL, Bytes.toBytes("www.mnop.com")));
      f2.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("h"),
        CompareOp.EQUAL, Bytes.toBytes("www.qrst.com")));

      scan.setFilter(f2);
      
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("h"));
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("w"));
      scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"));
    }
      break;

    default:
      break;

    }


    long startTime = System.currentTimeMillis();
    int count = 0;

    if (print) {
      Batch.Call<IzpScanTestProtocol, Result[]> callable =
          new Batch.Call<IzpScanTestProtocol, Result[]>() {
            public Result[] call(IzpScanTestProtocol instance) throws IOException {
              return instance.scan(scan, time[0], time[1]);
            }
          };
      Map<byte[], Result[]> results =
          table.coprocessorExec(IzpScanTestProtocol.class, scan.getStartRow(), scan.getStopRow(),
            callable);
      for (Result[] result : results.values()) {
        if (result != null && result.length != 0) {
          count += result.length;
          for (Result r : result) {
            println(r);
          }
        }
      }

    } else {
      Batch.Call<IzpScanTestProtocol, Integer> callable =
          new Batch.Call<IzpScanTestProtocol, Integer>() {
            public Integer call(IzpScanTestProtocol instance) throws IOException {
              return instance.scan(scan, time[0], time[1], false);
            }
          };

      Map<byte[], Integer> results =
          table.coprocessorExec(IzpScanTestProtocol.class, scan.getStartRow(), scan.getStopRow(),
            callable);
      for (Integer result : results.values()) {
        count += result;
      }
    }

    long stopTime = System.currentTimeMillis();
    System.out.println("Time elapsed: " + (stopTime - startTime) + " ms, result count: " + count);
    table.close();
    
    threadPool.shutdown();
  }

  static void println(Result result) {
    StringBuilder sb = new StringBuilder();
    // f FLOW_TYPE String
    // h HOST String
    // a AID String
    // y AP_TYPE String
    // r REGION_ID String
    // g AD_GROUP_ID String
    // p AD_PLAN_ID String
    // o OWNER_ID String
    // s SIZE Int
    // w SHOW Int
    // c CLICK Int
    // p PUSH Int
    // i IP Int
    // u UV Int
    // c COST Double
    sb.append("row=" + Bytes.toString(result.getRow()));

    List<KeyValue> kv = null;

    String string_column[] = { "f", "h", "a", "y", "r", "g", "p", "o" };
    String int_column1[] = { "s", "w", "c" };
    String int_column2[] = { "p", "i", "u" };
    for (String column : string_column) {
      kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes(column));
      if (kv.size() != 0) {
        sb.append(", " + Bytes.toString(kv.get(0).getFamily()) + ":" + column + "="
            + Bytes.toString(kv.get(0).getValue()));
      }
    }

    for (String column : int_column1) {
      kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes(column));
      if (kv.size() != 0) {
        sb.append(", " + Bytes.toString(kv.get(0).getFamily()) + ":" + column + "="
            + Bytes.toInt(kv.get(0).getValue()));
      }
    }

    for (String column : int_column2) {
      kv = result.getColumn(Bytes.toBytes("q"), Bytes.toBytes(column));
      if (kv.size() != 0) {
        sb.append(", " + Bytes.toString(kv.get(0).getFamily()) + ":" + column + "="
            + Bytes.toInt(kv.get(0).getValue()));
      }
    }

    kv = result.getColumn(Bytes.toBytes("q"), Bytes.toBytes("c"));
    if (kv.size() != 0) {
      sb.append(", " + Bytes.toString(kv.get(0).getFamily()) + ":" + "c" + "="
          + Bytes.toDouble(kv.get(0).getValue()));
    }

    System.out.println(sb.toString());
  }
}
