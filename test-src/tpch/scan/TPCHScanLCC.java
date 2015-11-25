package tpch.scan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.index.client.RangeList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import tpch.put.TPCHConstants;
import tpch.put.TPCHConstants.TPCH_CF_INFO;
import doWork.LCCIndexConstant;

public class TPCHScanLCC extends TPCHScanBaseClass {

  public TPCHScanLCC(String confPath, String newAddedFile, String tableName, List<Range> ranges)
      throws IOException {
    super(confPath, newAddedFile, tableName, ranges);
//    System.out.println("coffey disable table at first: " + tableName);
//    admin.disableTable(tableName);
//    System.out.println("coffey enable table then: " + tableName);
//    admin.enableTable(tableName);
    table = new HTable(conf, tableName);
  }

  @Override
  public ResultScanner getScanner() throws IOException {
    Scan scan = new Scan();
    RangeList list = new RangeList();
    for (Range r : ranges) {
      list.addRange(r);
    }
    scan.setCacheBlocks(false);
    scan.setAttribute(LCCIndexConstant.SCAN_WITH_LCCINDEX, Writables.getBytes(list));
    return table.getScanner(scan);
  }

  @Override
  public void printString(Result result) {
    StringBuilder sb = new StringBuilder();
    List<TPCH_CF_INFO> list = TPCHConstants.getCFInfo();
    for (KeyValue kv : result.list()) {
      for (TPCH_CF_INFO info : list) {
        if (info.qualifier.equals(Bytes.toString(kv.getQualifier()))) {
          if (info.isIndex) {
            sb.append(LCCIndexConstant.mWinterToPrint(kv));
            sb.append("\t");
          }
        }
      }
    }
    System.out.println(sb.toString());
  }
}
