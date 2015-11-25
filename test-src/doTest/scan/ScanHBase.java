package doTest.scan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.util.Bytes;

import doTest.put.PutTestConstants;
import doTest.put.PutTestConstants.CF_INFO;
import doWork.LCCIndexConstant;

public class ScanHBase extends ClassScanBase {

  public ScanHBase(String confPath, String newAddedFile, String tableName, int recordNumber,
      double percentage, String outputPath) throws IOException {
    super(confPath, newAddedFile, tableName, recordNumber, percentage, outputPath);
  }

  @Override
  public ResultScanner getScanner() throws IOException {
    Scan scan = new Scan();
    FilterList filters = new FilterList();
    List<CF_INFO> cfs = PutTestConstants.getCFInfo();
    for (CF_INFO ci : cfs) {
      if (ci.isIndex) {
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes(FAMILY_NAME), Bytes
            .toBytes(ci.qualifier), CompareOp.GREATER_OR_EQUAL, LCCIndexConstant
            .parsingStringToBytesWithType(ci.type, String.valueOf(startValue))));
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes(FAMILY_NAME), Bytes
            .toBytes(ci.qualifier), CompareOp.LESS_OR_EQUAL, LCCIndexConstant
            .parsingStringToBytesWithType(ci.type, String.valueOf(stopValue))));
        System.out.println("coffey hbase add filter for type: " + ci.type + " [" + startValue + ","
            + stopValue + "]");
      }
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

    List<CF_INFO> cfs = PutTestConstants.getCFInfo();
    for (CF_INFO ci : cfs) {
      kv = result.getColumn(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(ci.qualifier));
      if (kv.size() != 0 && ci.type != DataType.STRING) {
        sb.append(", [" + FAMILY_NAME + ":" + ci.qualifier + "]="
            + LCCIndexConstant.getStringOfValueAndType(ci.type, (kv.get(0).getValue())));
      }
    }
    System.out.println(sb.toString());
  }

  @Override
  public boolean insertResultForCheck(Result result) throws IOException {
    CheckedInfo info = null;
    List<KeyValue> kvs = null;
    String row = Bytes.toString(result.getRow());
    if (mapForCheck.containsKey(row)) {
      info = mapForCheck.get(row);
    }
    if (info == null) {
      info = new CheckedInfo();
    }
    for (CF_INFO ci : cfs) {
      kvs =
          result
              .getColumn(Bytes.toBytes(PutTestConstants.FAMILY_NAME), Bytes.toBytes(ci.qualifier));
      if (kvs.size() > 0) {
        if (info.hasValue.get(ci.qualifier) == true) {
          System.out.println("error meet same on key " + row + ", qualifier: " + ci.qualifier
              + " current row is: " + row);
          return false;
        }
        info.hasValue.put(ci.qualifier, true);
        if (writer != null && ci.type != DataType.STRING) {
          // key \t qualifier \t value
          writer.write(row + "\t" + ci.qualifier + "\t"
              + LCCIndexConstant.getStringOfValueAndType(ci.type, kvs.get(0).getValue()) + "\n");
        }
      }
    }
    info.checkFull();
    mapForCheck.put(row, info);
    return true;
  }
}
