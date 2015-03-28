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

public class IzpScanTest {
  public static void main(String[] args) throws IOException {
    HTable table = new HTable(HBaseConfiguration.create(), "izp30e");

    Scan scan = new Scan();
    FilterList f1 = new FilterList(Operator.MUST_PASS_ALL);
    
    f1.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("h"),
      CompareOp.EQUAL, Bytes.toBytes("www.pqai.com")));
    
    f1.addFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("y"),
      CompareOp.EQUAL, Bytes.toBytes("C8")));
    
    scan.setFilter(f1);
    scan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Bytes.toBytes(true));
    scan.setAttribute(IndexConstants.MAX_SCAN_SCALE, Bytes.toBytes(0.3f));

    scan.setCacheBlocks(false);
    scan.setCaching(100000);
//    scan.setStopRow(Bytes.toBytes("13"));

    ResultScanner scanner=table.getScanner(scan);
    Result result = null;
    long startTime = System.currentTimeMillis();
    int count = 0;
    boolean print=true;
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

    List<KeyValue> kv=null;
    
    String string_column[]={"f","h","a","y","r","g","p","o"};
    String int_column1[]={"s","w","c"};
    String int_column2[]={"p","i","u"};
    for(String column:string_column){      
      kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes(column));
      if (kv.size() != 0) {
        sb.append(", "+Bytes.toString(kv.get(0).getFamily())+":"+column+"=" + Bytes.toString(kv.get(0).getValue()));
      }
    }
    
    for(String column:int_column1){      
      kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes(column));
      if (kv.size() != 0) {
        sb.append(", "+Bytes.toString(kv.get(0).getFamily())+":"+column+"=" + Bytes.toInt(kv.get(0).getValue()));
      }
    }
    
    for(String column:int_column2){      
      kv = result.getColumn(Bytes.toBytes("q"), Bytes.toBytes(column));
      if (kv.size() != 0) {
        sb.append(", "+Bytes.toString(kv.get(0).getFamily())+":"+column+"=" + Bytes.toInt(kv.get(0).getValue()));
      }
    }
    
   
    kv = result.getColumn(Bytes.toBytes("q"), Bytes.toBytes("c"));
    if (kv.size() != 0) {
      sb.append(", "+Bytes.toString(kv.get(0).getFamily())+":"+"c"+"=" + Bytes.toDouble(kv.get(0).getValue()));
    }
    
    System.out.println(sb.toString());
  }
}
