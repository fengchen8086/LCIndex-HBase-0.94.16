package doTest.scan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ccindex.IndexTable;
import org.apache.hadoop.hbase.ccindex.SimpleIndexKeyGenerator;
import org.apache.hadoop.hbase.client.HTable;
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

public class ScanCCIndex extends ClassScanBase {

  private String mainIndexColumn;
  private DataType mainIndexType;

  public ScanCCIndex(String confPath, String newAddedFile, String tableName, int recordNumber,
      double percentage, String outputPath) throws IOException {
    super(confPath, newAddedFile, tableName, recordNumber, percentage, outputPath);
  }

  @Override
  public ResultScanner getScanner() throws IOException {
    IndexTable indexTable = new IndexTable(HBaseConfiguration.create(), tableName);
    HTable tableToScan = null;
    boolean hasSetMainQueryColumn = false;
    Scan scan = new Scan();
    FilterList filters = new FilterList();
    for (CF_INFO ci : cfs) {
      if (ci.isIndex) {
        if (!hasSetMainQueryColumn) {
          // set table to scan
          hasSetMainQueryColumn = true;
          tableToScan =
              indexTable.indexTableMaps.get(Bytes.toBytes(FAMILY_NAME + ":" + ci.qualifier));
          mainIndexColumn = ci.qualifier;
          mainIndexType = ci.type;
          // here we may meet problem on the start and stop key, because the real key may be
          // changed!
          scan.setStartRow(LCCIndexConstant.parsingStringToBytesWithType(ci.type,
            String.valueOf(startValue)));
          scan.setStopRow(LCCIndexConstant.parsingStringToBytesWithType(ci.type,
            String.valueOf(stopValue)));
          System.out.println("coffey main index range: " + ci.type + " [" + startValue + ","
              + stopValue + "]");
        } else {
          // set filters
          filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes(FAMILY_NAME), Bytes
              .toBytes(ci.qualifier), CompareOp.GREATER_OR_EQUAL, LCCIndexConstant
              .parsingStringToBytesWithType(ci.type, String.valueOf(startValue))));
          filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes(FAMILY_NAME), Bytes
              .toBytes(ci.qualifier), CompareOp.LESS_OR_EQUAL, LCCIndexConstant
              .parsingStringToBytesWithType(ci.type, String.valueOf(stopValue))));
          System.out.println("coffey add filter for type: " + ci.type + " [" + startValue + ","
              + stopValue + "]");
        }
      }
    }
    scan.setCacheBlocks(false);
    scan.setFilter(filters);
    return tableToScan.getScanner(scan);
  }

  @Override
  public void printString(Result result) {
    StringBuilder sb = new StringBuilder();
    List<KeyValue> kv = null;
    SimpleIndexKeyGenerator kg = new SimpleIndexKeyGenerator();
    sb.append("row=" + LCCIndexConstant.getStringOfValueAndType(mainIndexType, result.getRow()));
    byte[][] bytes = kg.parseIndexRowKey(result.getRow());
    sb.append(", key=" + LCCIndexConstant.getStringOfValueAndType(DataType.STRING, bytes[0]));
    sb.append(", value=" + LCCIndexConstant.getStringOfValueAndType(mainIndexType, bytes[1]));

    List<CF_INFO> cfs = PutTestConstants.getCFInfo();
    for (CF_INFO ci : cfs) {
      if (ci.qualifier.equals(mainIndexColumn)) {
        continue;
      }
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
    return false;
  }
}
