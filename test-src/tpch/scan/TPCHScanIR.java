package tpch.scan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.IndexConstants;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.index.client.RangeList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import tpch.put.TPCHConstants;
import doWork.LCCIndexConstant;

public class TPCHScanIR extends TPCHScanBaseClass {

  public TPCHScanIR(String confPath, String newAddedFile, String tableName, List<Range> ranges)
      throws IOException {
    super(confPath, newAddedFile, tableName, ranges);
  }

  @Override
  public ResultScanner getScanner() throws IOException {
    Scan scan = new Scan();
    RangeList list = new RangeList();
    FilterList filters = new FilterList();
    for (Range r : ranges) {
      list.addRange(r);
      if (r.getStartValue() != null) {
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes(TPCHConstants.FAMILY_NAME), r
            .getQualifier(), r.getStartType(), r.getStartValue()));
      }
      if (r.getStopValue() != null) {
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes(TPCHConstants.FAMILY_NAME), r
            .getQualifier(), r.getStopType(), r.getStopValue()));
      }
    }
    scan.setFilter(filters);
    scan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Writables.getBytes(list));
    scan.setAttribute(IndexConstants.MAX_SCAN_SCALE, Bytes.toBytes(0.3));
    scan.setCacheBlocks(false);
    return table.getScanner(scan);
  }

  @Override
  public void printString(Result result) {
    StringBuilder sb = new StringBuilder();
    for (KeyValue kv : result.list()) {
      sb.append(LCCIndexConstant.mWinterToPrint(kv));
      sb.append("\t");
    }
    System.out.println(sb.toString());
  }
}
