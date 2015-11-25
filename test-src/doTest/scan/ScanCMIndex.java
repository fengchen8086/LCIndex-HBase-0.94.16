package doTest.scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ccindex.IndexTable;
import org.apache.hadoop.hbase.ccindex.SimpleIndexKeyGenerator;
import org.apache.hadoop.hbase.client.Get;
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

public class ScanCMIndex extends ClassScanBase {

  public ScanCMIndex(String confPath, String newAddedFile, String tableName, int recordNumber,
      double percentage, String outputPath) throws IOException {
    super(confPath, newAddedFile, tableName, recordNumber, percentage, outputPath);
  }

  @Override
  public ResultScanner getScanner() throws IOException {
    if (stopValue > 10000) {
      return new CMIndexReadOneFilterScanner(HBaseConfiguration.create(), tableName);
    } else {
      return new CMIndexReadAllFilterScanner(conf, tableName);
    }
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
    // this is not necessary for CMIndex
    return false;
  }

  public class CMIndexReadOneFilterScanner implements ResultScanner {
    FilterList filters = new FilterList();
    private IndexTable indexTable;
    private HTable rawTable;
    private HTable scannedTable;
    private ResultScanner indexScanner;

    Configuration conf;
    String tableName;
    SimpleIndexKeyGenerator keyGen = new SimpleIndexKeyGenerator();

    public CMIndexReadOneFilterScanner(Configuration conf, String tableName) throws IOException {
      this.conf = conf;
      this.tableName = tableName;
      init();
    }

    private void init() throws IOException {
      rawTable = new HTable(HBaseConfiguration.create(), tableName);
      indexTable = new IndexTable(HBaseConfiguration.create(), tableName);
      boolean hasSetMainQueryColumn = false;
      Scan scan = new Scan();
      for (CF_INFO ci : cfs) {
        if (ci.isIndex) {
          if (!hasSetMainQueryColumn) {
            // set table to scan
            hasSetMainQueryColumn = true;
            scannedTable =
                indexTable.indexTableMaps.get(Bytes.toBytes(FAMILY_NAME + ":" + ci.qualifier));
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
      indexScanner = scannedTable.getScanner(scan);
    }

    @Override
    public Iterator<Result> iterator() {
      System.out.println("winter public Iterator<Result> iterator() not implemented");
      return null;
    }

    @Override
    public Result next() throws IOException {
      Result[] results = next(1);
      if (results == null || results.length < 1) {
        return null;
      }
      return results[0];
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
      // select the raw key first and use it to filter!
      // low throughput in this way!
      ArrayList<Result> list = new ArrayList<Result>();
      Result[] results;
      ArrayList<Get> getList = new ArrayList<Get>();
      while (true) {
        results = indexScanner.next(nbRows);
        if (results == null || results.length == 0) {
          break;
        }
        getList.clear();
        for (Result rs : results) {
          byte[][] columns = keyGen.parseIndexRowKey(rs.getRow());
          Get get = new Get(columns[0]);
          get.setCacheBlocks(false);
          get.setFilter(filters);
          getList.add(get);
        }
        Result[] realResults = rawTable.get(getList);
        for (Result rsIn : realResults) {
          if (rsIn == null || rsIn.getRow() == null) {
            continue;
          } else {
            list.add(rsIn);
            if (list.size() == nbRows) {
              break;
            }
          }
        }
      }
      return list.toArray(new Result[list.size()]);
    }

    @Override
    public void close() {
      try {
        indexScanner.close();
        rawTable.close();
        scannedTable.close();
        indexTable.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  // high latency but high throughput at the same time
  public class CMIndexReadAllFilterScanner implements ResultScanner {
    private IndexTable indexTable;
    private HTable rawTable;
    private TreeMap<byte[], ResultScanner> scannerMap = new TreeMap<byte[], ResultScanner>(
        Bytes.BYTES_COMPARATOR);
    private TreeMap<byte[], HashSet<String>> resultMap = new TreeMap<byte[], HashSet<String>>(
        Bytes.BYTES_COMPARATOR);

    private Queue<String> finalResults = new LinkedList<String>();

    private byte[] minimalSizeColumn = null;

    Configuration conf;
    String tableName;
    SimpleIndexKeyGenerator keyGen = new SimpleIndexKeyGenerator();

    public CMIndexReadAllFilterScanner(Configuration conf, String tableName) throws IOException {
      this.conf = conf;
      this.tableName = tableName;
      init();
    }

    private void init() throws IOException {
      rawTable = new HTable(HBaseConfiguration.create(), tableName);
      indexTable = new IndexTable(HBaseConfiguration.create(), tableName);
      Scan scan = new Scan();
      for (CF_INFO ci : cfs) {
        if (ci.isIndex) {
          HTable tableToScan =
              indexTable.indexTableMaps.get(Bytes.toBytes(FAMILY_NAME + ":" + ci.qualifier));
          scan.setStartRow(LCCIndexConstant.parsingStringToBytesWithType(ci.type,
            String.valueOf(startValue)));
          scan.setStopRow(LCCIndexConstant.parsingStringToBytesWithType(ci.type,
            String.valueOf(stopValue)));
          System.out.println("coffey main index range: " + ci.type + " [" + startValue + ","
              + stopValue + "]");
          scan.setCacheBlocks(false);
          scannerMap.put(Bytes.toBytes(FAMILY_NAME + ":" + ci.qualifier),
            tableToScan.getScanner(scan));
        }
      }
    }

    private void getAllResults() throws IOException {
      System.out.println("call getAllResults");
      int minimalSize = 0;
      int oneTimeCacheSize = 10000;
      Result[] results = null;
      for (Entry<byte[], ResultScanner> entry : scannerMap.entrySet()) {
        HashSet<String> resultSet = new HashSet<String>();
        System.out.println("get results from table: " + Bytes.toString(entry.getKey()));
        while (true) {
          results = entry.getValue().next(oneTimeCacheSize);
          if (results == null || results.length == 0) break;
          for (Result res : results) {
            // System.out.println("winter proper rowkey: "
            // + Bytes.toString(keyGen.parseIndexRowKey(res.getRow())[0]));
            resultSet.add(Bytes.toString(keyGen.parseIndexRowKey(res.getRow())[0]));
          }
        }
        resultMap.put(entry.getKey(), resultSet);
        System.out.println("recent size: " + resultSet.size());
        if (minimalSizeColumn == null || minimalSize > resultSet.size()) {
          minimalSizeColumn = entry.getKey();
          minimalSize = resultSet.size();
        }
      }
      for (ResultScanner rs : scannerMap.values()) {
        rs.close();
      }
      scannerMap = null;
    }

    private void mergeAllResults() {
      System.out.println("call mergeAllResults");
      HashSet<String> smallestSet = resultMap.get(minimalSizeColumn);
      resultMap.remove(minimalSizeColumn);
      boolean containThisKey;
      for (String key : smallestSet) {
        containThisKey = true;
        for (HashSet<String> set : resultMap.values()) {
          if (!set.contains(key)) {
            containThisKey = false;
            break;
          }
        }
        if (containThisKey) {
          finalResults.add(key);
        }
      }
      System.out.println("final result size: " + finalResults.size());
      resultMap = null;
    }

    @Override
    public Iterator<Result> iterator() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Result next() throws IOException {
      Result[] results = next(1);
      if (results == null || results.length < 1) {
        return null;
      }
      return results[0];
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
      if (minimalSizeColumn == null) {
        getAllResults();
        mergeAllResults();
      }
      int realLen = finalResults.size() < nbRows ? finalResults.size() : nbRows;
      ArrayList<Get> getList = new ArrayList<Get>();
      while (realLen-- > 0) {
        getList.add(new Get(Bytes.toBytes(finalResults.poll())));
      }
      return rawTable.get(getList);
    }

    @Override
    public void close() {
      try {
        rawTable.close();
        indexTable.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      if (scannerMap != null) {
        for (ResultScanner rs : scannerMap.values()) {
          rs.close();
        }
        scannerMap = null;
      }
      resultMap = null;
    }
  }

}
