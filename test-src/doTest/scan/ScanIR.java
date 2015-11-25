package doTest.scan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.index.client.IndexConstants;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.index.client.RangeList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import doTest.put.PutTestConstants;
import doTest.put.PutTestConstants.CF_INFO;
import doWork.LCCIndexConstant;

public class ScanIR extends ClassScanBase {

  public ScanIR(String confPath, String newAddedFile, String tableName, int recordNumber,
      double percentage, String outputPath) throws IOException {
    super(confPath, newAddedFile, tableName, recordNumber, percentage, outputPath);
  }

  @Override
  public ResultScanner getScanner() throws IOException {
    Scan scan = new Scan();
    RangeList list = new RangeList();
    List<CF_INFO> cfs = PutTestConstants.getCFInfo();
    FilterList filters = new FilterList();
    for (CF_INFO ci : cfs) {
      if (ci.isIndex) {
        list.addRange(new Range(Bytes.toBytes(FAMILY_NAME + ":" + ci.qualifier), LCCIndexConstant
            .parsingStringToBytesWithType(ci.type, String.valueOf(startValue)),
            CompareOp.GREATER_OR_EQUAL, LCCIndexConstant.parsingStringToBytesWithType(ci.type,
              String.valueOf(stopValue)), CompareOp.LESS_OR_EQUAL));
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes(FAMILY_NAME), Bytes
            .toBytes(ci.qualifier), CompareOp.GREATER_OR_EQUAL, LCCIndexConstant
            .parsingStringToBytesWithType(ci.type, String.valueOf(startValue))));
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes(FAMILY_NAME), Bytes
            .toBytes(ci.qualifier), CompareOp.LESS_OR_EQUAL, LCCIndexConstant
            .parsingStringToBytesWithType(ci.type, String.valueOf(stopValue))));
        System.out.println("coffey irindex add filter for type: " + ci.type + " [" + startValue
            + "," + stopValue + "]");
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
    for (KeyValue kv : result.list()) {
      System.out.println(LCCIndexConstant.mWinterToPrint(kv));
    }
  }

  @Override
  public boolean insertResultForCheck(Result result) {
    return false;
  }
}
