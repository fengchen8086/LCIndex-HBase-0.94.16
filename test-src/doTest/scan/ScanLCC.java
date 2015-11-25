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
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.index.client.RangeList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import doTest.put.PutTestConstants;
import doTest.put.PutTestConstants.CF_INFO;
import doWork.LCCIndexConstant;

public class ScanLCC extends ClassScanBase {

  public ScanLCC(String confPath, String newAddedFile, String tableName, int recordNumber,
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
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("if_A"), Bytes
            .toBytes(ci.qualifier), CompareOp.GREATER_OR_EQUAL, LCCIndexConstant
            .parsingStringToBytesWithType(ci.type, String.valueOf(startValue))));
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("if_A"), Bytes
            .toBytes(ci.qualifier), CompareOp.LESS_OR_EQUAL, LCCIndexConstant
            .parsingStringToBytesWithType(ci.type, String.valueOf(stopValue))));
        System.out.println("coffey lccindex add filter for type: " + ci.type + " [" + startValue
            + "," + stopValue + "]");
      }
    }
    scan.setCacheBlocks(false);
    scan.setFilter(filters);
    scan.setAttribute(LCCIndexConstant.SCAN_WITH_LCCINDEX, Writables.getBytes(list));
    return table.getScanner(scan);
  }

  @Override
  public void printString(Result result) {
    for (KeyValue kv : result.list()) {
      System.out.println(LCCIndexConstant.mWinterToPrint(kv));
    }
  }

  @Override
  public boolean insertResultForCheck(Result result) throws IOException {
    String row = Bytes.toString(result.getRow());
    String parts[] = row.split("#");
    CheckedInfo info = null;
    List<KeyValue> kvs = null;
    if (mapForCheck.containsKey(parts[2])) {
      info = mapForCheck.get(parts[2]);
    }
    if (info == null) {
      info = new CheckedInfo();
    }
    // check the default!
    info.hasValue.put(parts[1], true);
    if (writer != null) {
      // key \t qualifier \t value
      writer.write(parts[2]
          + "\t"
          + parts[1]
          + "\t"
          + LCCIndexConstant.getStringOfValueAndType(getType(parts[1]),
            LCCIndexConstant.parsingStringToBytesWithType(getType(parts[1]), parts[0])) + "\n");
    }

    for (CF_INFO ci : cfs) {
      if (ci.qualifier.equalsIgnoreCase(parts[1])) {
        continue;
      }
      kvs =
          result.getColumn(Bytes.toBytes(LCCIndexConstant.CF_FAMILY_PREFIX_STR + parts[1]),
            Bytes.toBytes(ci.qualifier));
      if (kvs.size() > 0) {
        if (info.hasValue.get(ci.qualifier) == true) {
          System.out.println("error meet same on key " + parts[2] + ", qualifier: " + ci.qualifier
              + " current row is: " + row);
          return false;
        }
        info.hasValue.put(ci.qualifier, true);
        if (writer != null && ci.type != DataType.STRING) {
          // key \t qualifier \t value
          writer.write(parts[2] + "\t" + ci.qualifier + "\t"
              + LCCIndexConstant.getStringOfValueAndType(ci.type, kvs.get(0).getValue()) + "\n");
        }
      }
    }
    info.checkFull();
    mapForCheck.put(parts[2], info);
    return true;
  }
}
