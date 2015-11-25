package tpch.scan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.util.Bytes;

import tpch.put.TPCHConstants;
import tpch.put.TPCHConstants.TPCH_CF_INFO;
import doWork.LCCIndexConstant;

public class TPCHScanHBase extends TPCHScanBaseClass {

  public TPCHScanHBase(String confPath, String newAddedFile, String tableName, List<Range> ranges)
      throws IOException {
    super(confPath, newAddedFile, tableName, ranges);
  }

  @Override
  public ResultScanner getScanner() throws IOException {
    Scan scan = new Scan();
    FilterList filters = new FilterList();
    for (Range range : ranges) {
      if (range.getStartValue() != null) {
        filters.addFilter(new SingleColumnValueFilter(range.getFamily(), range.getQualifier(),
            range.getStartType(), range.getStartValue()));
      }
      if (range.getStopValue() != null) {
        filters.addFilter(new SingleColumnValueFilter(range.getFamily(), range.getQualifier(),
            range.getStopType(), range.getStopValue()));
      }
      System.out.println("coffey hbase main index range: " + Bytes.toString(range.getColumn())
          + " ["
          + LCCIndexConstant.getStringOfValueAndType(range.getDataType(), range.getStartValue())
          + ","
          + LCCIndexConstant.getStringOfValueAndType(range.getDataType(), range.getStopValue())
          + "]");
      scan.setCacheBlocks(false);
    }
    scan.setCacheBlocks(false);
    scan.setFilter(filters);
    return table.getScanner(scan);
  }

  @Override
  public void printString(Result result) {
    StringBuilder sb = new StringBuilder();
    List<KeyValue> kv = null;
    sb.append("row=" + Bytes.toString(result.getRow()));

    List<TPCH_CF_INFO> cfs = TPCHConstants.getCFInfo();
    for (TPCH_CF_INFO ci : cfs) {
      kv = result.getColumn(Bytes.toBytes(TPCHConstants.FAMILY_NAME), Bytes.toBytes(ci.qualifier));
      if (kv.size() != 0) {
        sb.append(", [" + TPCHConstants.FAMILY_NAME + ":" + ci.qualifier + "]="
            + LCCIndexConstant.getStringOfValueAndType(ci.type, (kv.get(0).getValue())));
      }
    }
    System.out.println(sb.toString());
  }
}
