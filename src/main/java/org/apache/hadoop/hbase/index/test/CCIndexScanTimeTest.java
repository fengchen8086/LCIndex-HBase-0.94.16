package org.apache.hadoop.hbase.index.test;

import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ccindex.IndexTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class CCIndexScanTimeTest {
  public static void main(String[] args) throws Exception {
    IndexTable table = new IndexTable(HBaseConfiguration.create(), "orders");
    table.setMaxGetsPerScan(1);
    table.setMaxScanThreads(1);
    table.setScannerCaching(100000);
    table.setUseIndex(true);
    
    boolean printProgress = false;
    boolean print = false;
    
    double[] c3_value = 
        new double[] {1500.0, 3000.0, 7000.0, 15000.0, 25000.0, 50000.0, 60000.0, 70000.0,
        90000.0,120000.0,150000.0,170000.0, 200000.0, 230000.0, 250000.0,500000.0};
        
        
//     new double[] {1000.0, 1500.0, 2000.0, 3000.0, 5000.0, 7000.0, 10000.0, 13000.0, 15000.0,
//     20000.0, 25000.0, 30000.0, 35000.0, 40000.0, 45000.0, 50000.0, 60000.0, 70000.0,
//     80000.0,90000.0,120000.0,150000.0,170000.0};
    
    
//        new double[] {1000.0, 50000.0, 60000.0,};
    
//    String[] c5_value =new String[]{"1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW"};
    for (double v : c3_value) {
      System.out.println(v);
      double c3_end = v;
      String c5_equal =
          "1-URGENT";

      
      table.query(c3_end, c5_equal, printProgress, print);
      
/*
      ArrayList<Range> list = new ArrayList<Range>();
      if (c3_end > 0) {
        list.add(new Range(Bytes.toBytes("orders"),Bytes.toBytes("f:c3"), null, CompareOp.NO_OP,
          Bytes.toBytes(c3_end), CompareOp.LESS));
      }

      if (c5_equal != null) {
        list.add(new Range(Bytes.toBytes("orders"),Bytes.toBytes("f:c5"), Bytes.toBytes(c5_equal), CompareOp.EQUAL,
          null, CompareOp.NO_OP));
      }
      
      IndexQuerySQL sql=new IndexQuerySQL(Bytes.toBytes("orders"),(byte[][])null,new Range[][]{list.toArray(new Range[0])});

      long startTime = System.currentTimeMillis();
      
      IndexResultScanner scanner = table.getScanner(sql);
      int count = 0;
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
 */
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
