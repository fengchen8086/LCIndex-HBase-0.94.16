package org.apache.hadoop.hbase.ccindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ccindex.IndexResultScanner.IndexSingleScanner;
import org.apache.hadoop.hbase.ccindex.IndexResultScanner.NoIndexSingleScanner;
import org.apache.hadoop.hbase.ccindex.IndexResultScanner.SingleScanner;
import org.apache.hadoop.hbase.ccindex.IndexSpecification.IndexType;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.test.CCIndexScanThroughputTest;
import org.apache.hadoop.hbase.util.Bytes;

import doWork.LCCIndexConstant;

/**
 * Extended from HTable with indexed support.
 * @author wanhao
 */
public class IndexTable {
  private static final Log LOG = LogFactory.getLog(IndexTable.class);
  private Configuration conf;
  private long writeBufferSize;
  private int scannerCaching;
  private boolean autoFlush;

  private final byte[] tableName;
  private final IndexTableDescriptor indexDesc;
  private IndexChooser chooser;
  private HTable mainTable;
  private HTable mainCCTTable;

  public Map<byte[], HTable> indexTableMaps = null;
  public Map<byte[], HTable> cctTableMaps = null;
  public static byte[] CCT_FIX = Bytes.toBytes("_cct");

  // column info of this table, mainly contains each column's type
  private Map<byte[], DataType> columnTypeMap = null;

  // max scan threads for an IndexResultScanner, the actually run scan threads
  // is no more than this
  private int maxScanners = 0;
  private static int DEFAULT_MAX_SCANNERS = 10;

  // max get threads for a scan, it will be used when SecIndex or ImpSecIndex
  // if chose to scan
  // and index table doesn't contain all result columns
  private int maxGetsPerScanner = 0;
  private static int DEFAULT_MAX_GETS_PER_SCANNER = 10;

  // result buffer size for IndexResultScanner
  private int resultBufferSize = 0;
  private static int DEFAULT_RESULT_BUFFER_SIZE = 10000;

  // when buffer has less than resultBufferSize*loadFactor results,
  // start the interrupted scan threads if scan is not ended
  private float loadFactor = 0.0f;
  private static float DEFAULT_LOAD_FACTOR = 0.75f;

  // if you want to use index for scan
  private boolean useIndex = true;

  /**
   * Construct with default configuration.
   * @param tableName
   * @throws IOException
   */
  public IndexTable(final byte[] tableName) throws IOException {
    this(HBaseConfiguration.create(), tableName);
  }

  /**
   * Construct with default configuration.
   * @param tableName
   * @throws IOException
   */
  public IndexTable(final String tableName) throws IOException {
    this(HBaseConfiguration.create(), Bytes.toBytes(tableName));
  }

  /**
   * Construct with given configuration.
   * @param conf
   * @param tableName
   * @throws IOException
   */
  public IndexTable(final Configuration conf, final String tableName) throws IOException {
    this(conf, Bytes.toBytes(tableName));
  }

  /**
   * Construct with given configuration.
   * @param conf
   * @param tableName
   * @throws IOException
   */
  public IndexTable(final Configuration conf, final byte[] tableName) throws IOException {
    this.conf = conf;
    this.tableName = tableName;
    this.writeBufferSize = conf.getLong("hbase.client.write.buffer", 2097152);
    this.autoFlush = true;
    this.scannerCaching = conf.getInt("hbase.client.scanner.caching", 1000);

    this.mainTable = new HTable(conf, tableName);
    this.mainCCTTable = new HTable(conf, Bytes.add(tableName, CCT_FIX));
    this.indexDesc = new IndexTableDescriptor(mainTable.getTableDescriptor());
    this.indexTableMaps = new TreeMap<byte[], HTable>(Bytes.BYTES_COMPARATOR);
    this.cctTableMaps = new TreeMap<byte[], HTable>(Bytes.BYTES_COMPARATOR);

    indexTableMaps.put(IndexConstants.KEY, mainTable);
    cctTableMaps.put(Bytes.add(IndexConstants.KEY, CCT_FIX), mainCCTTable);
    if (indexDesc.getIndexedColumns() != null && indexDesc.getIndexedColumns().length != 0) {
      for (IndexSpecification spec : indexDesc.getIndexSpecifications()) {
        indexTableMaps.put(spec.getIndexColumn(), new HTable(conf, spec.getIndexTableName()));
        cctTableMaps.put(Bytes.add(spec.getIndexColumn(), CCT_FIX),
          new HTable(conf, Bytes.add(spec.getIndexTableName(), CCT_FIX)));
      }
    }
    String tempInfo = mainTable.getTableDescriptor().getValue("DATA_FORMAT");
    if (tempInfo != null) {
      this.columnTypeMap = new TreeMap<byte[], DataType>(Bytes.BYTES_COMPARATOR);
      String[] temp = tempInfo.split(",");
      for (int i = 0; i < temp.length; i++) {
        int loc = temp[i].lastIndexOf(':');
        if (loc != -1) {
          this.columnTypeMap.put(Bytes.toBytes(temp[i].substring(0, loc)),
            DataType.valueOf(temp[i].substring(loc + 1)));
        } else {
          LOG.warn("Failed to read column type!" + temp[i]);
        }
      }
    }
    this.resultBufferSize = DEFAULT_RESULT_BUFFER_SIZE;
    this.loadFactor = DEFAULT_LOAD_FACTOR;
    this.maxScanners = DEFAULT_MAX_SCANNERS;
    this.maxGetsPerScanner = DEFAULT_MAX_GETS_PER_SCANNER;
    this.chooser = new SimpleIndexChooser(this);
  }

  /**
   * Put a single row into table.
   * <p>
   * If this table already has indexes, make sure that you don't put duplicate rows into it,
   * otherwise you may get inconsistency indexes.
   * @param put
   * @throws IOException
   */
  public void put(final Put put, boolean checkDuplicates) throws IOException {
    if (checkDuplicates) {
      Get get = null;
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (get == null) {
          get = new Get(put.getRow());
        }
        get.addColumn(indexSpec.getFamily(), indexSpec.getQualifier());
      }
      Result result = this.get(get);
      if (!result.isEmpty()) {
        IndexDelete indexdelete = IndexUtils.createIndexDelete(indexDesc, new Delete(put.getRow()),
          result);
        indexdelete.setWAL(put.getDurability());
        doIndexDelete(Arrays.asList(indexdelete));
      }
    }
    // here to modify !!!
    IndexPut indexput = IndexUtils.createIndexPut(indexDesc, put);
    indexput.setWAL(put.getDurability());
    doIndexPut(Arrays.asList(indexput));
  }

  private void doIndexPut(final List<IndexPut> puts) throws IOException {
    HTable temptable = null;
    HTable tempCCT = null;

    for (IndexPut put : puts) {
      for (Map.Entry<byte[], Put> entry : put.getPuts().entrySet()) {
        temptable = indexTableMaps.get(entry.getKey());
        temptable.put(entry.getValue());
        Put cctPut = IndexUtils.parseCCTPut(indexDesc, entry.getValue());
        if (cctPut != null) {
          // System.out.println("winter indextable name: " + Bytes.toString(entry.getKey())
          // + ", values: " + entry.getValue() + ", cct value: " + cctPut);
          tempCCT = cctTableMaps.get(Bytes.add(entry.getKey(), CCT_FIX));
          tempCCT.put(cctPut);
        }
        // something to do here
      }
    }
  }

  /**
   * Deletes the specified cells/row.
   * <p>
   * For a table which already has indexes, only deleting row is supported currently. Make sure that
   * you only delete the whole row for a table with indexes.
   * @param delete The object that specifies what to delete.
   * @throws IOException -if a remote or network exception occurs.
   */
  public void delete(final Delete delete) throws IOException {
    IndexDelete indexdelete = null;

    if (indexDesc.hasIndex()) {
      Get get = null;
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (needDeleteIndexData(delete, indexSpec)) {
          if (get == null) {
            get = new Get(delete.getRow());
          }
          get.addColumn(indexSpec.getFamily(), indexSpec.getQualifier());
        }
      }

      if (get != null) {
        indexdelete = IndexUtils.createIndexDelete(indexDesc, delete, this.get(get));
      } else {
        indexdelete = IndexUtils.createIndexDelete(indexDesc, delete, null);
      }

    } else {
      indexdelete = IndexUtils.createIndexDelete(indexDesc, delete, null);
    }

    indexdelete.setWAL(delete.getDurability());

    doIndexDelete(Arrays.asList(indexdelete));
  }

  /**
   * If you need to delete index data.
   * @param delete
   * @param indexSpec
   * @return
   */
  private boolean needDeleteIndexData(Delete delete, IndexSpecification indexSpec) {
    if (delete.isEmpty()) {
      return true;
    } else {

      if (indexSpec.getIndexType() == IndexType.CCINDEX) {
        return true;
      }
      for (Map.Entry<byte[], List<KeyValue>> entry : delete.getFamilyMap().entrySet()) {
        for (KeyValue kv : entry.getValue()) {
          if (kv.getQualifierLength() == 0) // delete family
          {
            if (Bytes.compareTo(kv.getFamily(), indexSpec.getFamily()) == 0) {
              return true;
            }
            if (indexSpec.getIndexType() == IndexType.IMPSECONDARYINDEX) {
              if (indexSpec.getAdditionMap().containsKey(kv.getFamily())) {
                return true;
              }
            }

          } else {
            if (Bytes.compareTo(kv.getFamily(), indexSpec.getFamily()) == 0
                && Bytes.compareTo(kv.getQualifier(), indexSpec.getQualifier()) == 0) {
              return true;
            }
            if (indexSpec.getIndexType() == IndexType.IMPSECONDARYINDEX) {
              if (indexSpec.getAdditionMap().containsKey(kv.getFamily())) {
                Set<byte[]> qua = indexSpec.getAdditionMap().get(kv.getFamily());
                if (qua == null) {
                  return true;
                } else {
                  if (qua.contains(kv.getQualifier())) {
                    return true;
                  }
                }
              }
            }
          }
        }
      }

    }
    return false;
  }

  private void doIndexDelete(final List<IndexDelete> deletes) throws IOException {
    HTable temptable = null;
    for (IndexDelete delete : deletes) {
      for (Map.Entry<byte[], Delete> entry : delete.getDeletes().entrySet()) {
        temptable = indexTableMaps.get(entry.getKey());
        temptable.delete(entry.getValue());
      }
    }
  }

  public Result get(final Get get) throws IOException {
    return mainTable.get(get);
  }

  public Result[] get(List<Get> gets) throws IOException {
    return mainTable.get(gets);
  }

  /**
   * Get a result scanner for sql query.
   * @param indexSql
   * @param maxScanThreads
   * @return
   * @throws Exception
   */
  public IndexResultScanner getScanner(IndexQuerySQL indexSql, int maxScanThreads) throws Exception {
    this.setMaxScanThreads(maxScanThreads);
    return this.getScanner(indexSql);
  }

  /**
   * Get a result scanner for sql query.
   * @param indexSql
   * @param bufferSize
   * @param loadFactor
   * @param maxScanThreads
   * @return
   * @throws Exception
   */
  public IndexResultScanner getScanner(IndexQuerySQL indexSql, int bufferSize, float loadFactor,
      int maxScanThreads) throws Exception {
    this.setResultBufferSize(bufferSize);
    this.setLoadFactor(loadFactor);
    this.setMaxScanThreads(maxScanThreads);
    return this.getScanner(indexSql);
  }

  public void query(double c3_end, String c5_equal, boolean printProgress, boolean print)
      throws IOException, IndexNotExistedException {
    ArrayList<Range> list = new ArrayList<Range>();
    if (c3_end > 0) {
      list.add(new Range(Bytes.toBytes("orders"), Bytes.toBytes("f:c3"), null, CompareOp.NO_OP,
          Bytes.toBytes(c3_end), CompareOp.LESS));
    }

    if (c5_equal != null) {
      list.add(new Range(Bytes.toBytes("orders"), Bytes.toBytes("f:c5"), Bytes.toBytes(c5_equal),
          CompareOp.EQUAL, null, CompareOp.NO_OP));
    }

    // int index = chooser.whichToScan(list.toArray(new Range[0]),
    // (byte[][]) null);
    int index = 0;
    if (list.size() > 1 && c3_end > 60000.0) {
      index = 1;
    }

    if (IndexType.CCINDEX == this.indexDesc.getIndexSpecification(list.get(index).getColumn())
        .getIndexType()) {
      Scan scan = new Scan();
      scan.setCacheBlocks(false);
      scan.setCaching(100000);

      if (Bytes.equals(list.get(index).getColumn(), Bytes.toBytes("f:c3"))) {
        scan.setStopRow(list.get(index).getEndValue());
        if (list.size() > 1) {
          scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c5"),
              CompareOp.EQUAL, Bytes.toBytes(c5_equal)));
        }
      } else {
        scan.setStartRow(list.get(index).getStartValue());
        scan.setStopRow(Bytes.add(list.get(index).getStartValue(), IndexConstants.MAX_ROW_KEY));
        if (list.size() > 1) {
          scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),
              CompareOp.LESS, Bytes.toBytes(c3_end)));
        }
      }

      HTable table = this.indexTableMaps.get(list.get(index).getColumn());
      long startTime = System.currentTimeMillis();

      ResultScanner scanner = table.getScanner(scan);
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
    } else {
      Scan scan = new Scan();
      scan.setCacheBlocks(false);
      scan.setCaching(10000);

      Filter filter = null;

      if (Bytes.equals(list.get(index).getColumn(), Bytes.toBytes("f:c3"))) {
        scan.setStopRow(list.get(index).getEndValue());
        if (list.size() > 1) {
          filter = new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c5"),
              CompareOp.EQUAL, Bytes.toBytes(c5_equal));
        }
      } else {
        scan.setStartRow(list.get(index).getStartValue());
        scan.setStopRow(Bytes.add(list.get(index).getStartValue(), IndexConstants.MAX_ROW_KEY));
        if (list.size() > 1) {
          filter = new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),
              CompareOp.LESS, Bytes.toBytes(c3_end));
        }
      }

      HTable table = this.indexTableMaps.get(list.get(index).getColumn());
      long startTime = System.currentTimeMillis();

      ResultScanner scanner = table.getScanner(scan);
      int count = 0;
      Result[] result = null;

      while ((result = scanner.next(10000)) != null && result.length != 0) {
        ArrayList<Get> getlist = new ArrayList<Get>(result.length);
        for (Result r : result) {
          Get get = new Get(parseIndexRowKey(r.getRow()));
          get.setCacheBlocks(false);
          if (filter != null) {
            get.setFilter(filter);
          }
          getlist.add(get);
        }

        Result[] getresult = mainTable.get(getlist);
        if (getresult != null && getresult.length != 0) {
          for (Result gr : getresult) {
            if (!gr.isEmpty()) {
              count++;
              if (print) {
                println(gr);
              }

              if (printProgress && (count % 100000 == 0)) {
                System.out.println("Time elapsed: " + (System.currentTimeMillis() - startTime)
                    + " ms, result count: " + count);
              }
            }
          }
        }

      }

      long stopTime = System.currentTimeMillis();
      System.out.println("Time elapsed: " + (stopTime - startTime) + " ms, result count: " + count);
    }

  }

  public long query(double c3_end, String c5_equal, boolean print, int caching) throws IOException,
      IndexNotExistedException {
    ArrayList<Range> list = new ArrayList<Range>();
    if (c3_end > 0) {
      list.add(new Range(Bytes.toBytes("orders"), Bytes.toBytes("f:c3"), null, CompareOp.NO_OP,
          Bytes.toBytes(c3_end), CompareOp.LESS));
    }

    if (c5_equal != null) {
      list.add(new Range(Bytes.toBytes("orders"), Bytes.toBytes("f:c5"), Bytes.toBytes(c5_equal),
          CompareOp.EQUAL, null, CompareOp.NO_OP));
    }

    int index = 0;
    if (list.size() > 1 && c3_end > 60000.0) {
      index = 1;
    }

    if (IndexType.CCINDEX == this.indexDesc.getIndexSpecification(list.get(index).getColumn())
        .getIndexType()) {
      Scan scan = new Scan();
      scan.setCacheBlocks(false);
      scan.setCaching(caching);

      if (Bytes.equals(list.get(index).getColumn(), Bytes.toBytes("f:c3"))) {
        scan.setStopRow(list.get(index).getEndValue());
        if (list.size() > 1) {
          scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c5"),
              CompareOp.EQUAL, Bytes.toBytes(c5_equal)));
        }
      } else {
        scan.setStartRow(list.get(index).getStartValue());
        scan.setStopRow(Bytes.add(list.get(index).getStartValue(), IndexConstants.MAX_ROW_KEY));
        if (list.size() > 1) {
          scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),
              CompareOp.LESS, Bytes.toBytes(c3_end)));
        }
      }

      HTable table = this.indexTableMaps.get(list.get(index).getColumn());
      long startTime = System.currentTimeMillis();

      ResultScanner scanner = table.getScanner(scan);
      Result[] result = scanner.next(caching);

      long stopTime = System.currentTimeMillis();
      System.out.println("Time elapsed: " + (stopTime - startTime) + " ms, result count: "
          + result.length);

      scanner.close();
      return stopTime - startTime;
    } else {
      Scan scan = new Scan();
      scan.setCacheBlocks(false);
      scan.setCaching(caching > 10000 ? 10000 : caching);

      Filter filter = null;

      if (Bytes.equals(list.get(index).getColumn(), Bytes.toBytes("f:c3"))) {
        scan.setStopRow(list.get(index).getEndValue());
        if (list.size() > 1) {
          filter = new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c5"),
              CompareOp.EQUAL, Bytes.toBytes(c5_equal));
        }
      } else {
        scan.setStartRow(list.get(index).getStartValue());
        scan.setStopRow(Bytes.add(list.get(index).getStartValue(), IndexConstants.MAX_ROW_KEY));
        if (list.size() > 1) {
          filter = new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),
              CompareOp.LESS, Bytes.toBytes(c3_end));
        }
      }

      HTable table = this.indexTableMaps.get(list.get(index).getColumn());
      long startTime = System.currentTimeMillis();

      ResultScanner scanner = table.getScanner(scan);
      int count = 0;
      Result[] result = null;

      query: while ((result = scanner.next(caching > 10000 ? 10000 : caching)) != null
          && result.length != 0) {
        ArrayList<Get> getlist = new ArrayList<Get>(result.length);
        for (Result r : result) {
          Get get = new Get(parseIndexRowKey(r.getRow()));
          get.setCacheBlocks(false);
          if (filter != null) {
            get.setFilter(filter);
          }
          getlist.add(get);
        }

        Result[] getresult = mainTable.get(getlist);
        if (getresult != null && getresult.length != 0) {
          for (Result gr : getresult) {
            if (!gr.isEmpty()) {
              count++;
              if (print) {
                println(gr);
              }

              if (count >= caching) {
                break query;
              }
            }
          }
        }

      }

      long stopTime = System.currentTimeMillis();
      System.out.println("Time elapsed: " + (stopTime - startTime) + " ms, result count: " + count);

      table.close();
      return stopTime - startTime;
    }

  }

  public long query(CCIndexScanThroughputTest test, String name, double c3_end, String c5_equal,
      boolean print, int caching, byte[] start, byte[] stop) throws IOException,
      IndexNotExistedException {
    ArrayList<Range> list = new ArrayList<Range>();
    list.add(new Range(Bytes.toBytes("orders"), Bytes.toBytes("f:c3"), null, CompareOp.NO_OP, Bytes
        .toBytes(c3_end), CompareOp.LESS));
    list.add(new Range(Bytes.toBytes("orders"), Bytes.toBytes("f:c5"), Bytes.toBytes(c5_equal),
        CompareOp.EQUAL, null, CompareOp.NO_OP));

    int index = 1;
    if (IndexType.CCINDEX == this.indexDesc.getIndexSpecification(list.get(index).getColumn())
        .getIndexType()) {
      Scan scan = new Scan();
      scan.setCacheBlocks(false);
      scan.setCaching(caching);

      scan.setStartRow(Bytes.add(
        Bytes.add(list.get(index).getStartValue(), IndexConstants.MIN_ROW_KEY), start));
      scan.setStopRow(Bytes.add(
        Bytes.add(list.get(index).getStartValue(), IndexConstants.MIN_ROW_KEY), stop));
      scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"),
          CompareOp.LESS, Bytes.toBytes(c3_end)));

      HTable table = this.indexTableMaps.get(list.get(index).getColumn());
      long startTime = System.currentTimeMillis();

      ResultScanner scanner = table.getScanner(scan);
      Result[] result = null;
      int count = 0;
      while ((result = scanner.next(caching > 10000 ? caching : 10000)) != null
          && result.length != 0) {
        test.addResults(result.length, false);
        count += result.length;
        if (print) {
          for (Result r : result) {
            println(r);
          }
        }
      }
      test.addResults(0, true);

      long stopTime = System.currentTimeMillis();
      System.out.println(name + " throughput:" + count * 1000.0 / (stopTime - startTime));

      scanner.close();
      return stopTime - startTime;
    } else {
      Scan scan = new Scan();
      scan.setCacheBlocks(false);
      scan.setCaching(caching > 10000 ? 10000 : caching);

      Filter filter = null;

      scan.setStartRow(Bytes.add(
        Bytes.add(list.get(index).getStartValue(), IndexConstants.MIN_ROW_KEY), start));
      scan.setStopRow(Bytes.add(
        Bytes.add(list.get(index).getStartValue(), IndexConstants.MIN_ROW_KEY), stop));

      filter = new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c3"), CompareOp.LESS,
          Bytes.toBytes(c3_end));

      HTable table = this.indexTableMaps.get(list.get(index).getColumn());
      long startTime = System.currentTimeMillis();

      ResultScanner scanner = table.getScanner(scan);
      int count = 0;
      Result[] result = null;

      while ((result = scanner.next(caching > 10000 ? 10000 : caching)) != null
          && result.length != 0) {
        ArrayList<Get> getlist = new ArrayList<Get>(result.length);
        for (Result r : result) {
          Get get = new Get(parseIndexRowKey(r.getRow()));
          get.setCacheBlocks(false);
          if (filter != null) {
            get.setFilter(filter);
          }
          getlist.add(get);
        }

        Result[] getresult = mainTable.get(getlist);
        if (getresult != null && getresult.length != 0) {
          int newcount = 0;
          for (Result gr : getresult) {
            if (!gr.isEmpty()) {
              count++;
              newcount++;
              if (print) {
                println(gr);
              }
            }
          }
          test.addResults(newcount, false);
        }

      }

      test.addResults(0, true);

      long stopTime = System.currentTimeMillis();
      System.out.println(name + " throughput:" + count * 1000.0 / (stopTime - startTime));

      table.close();
      return stopTime - startTime;
    }

  }

  public byte[] parseIndexRowKey(byte[] indexKey) {
    int length = Integer.valueOf(
      Bytes.toString(Bytes.tail(indexKey, IndexConstants.LASTPART_LENGTH))).intValue();
    // get row key of main data table
    byte[] result = new byte[indexKey.length - IndexConstants.LASTPART_LENGTH - length];
    for (int i = 0; i < result.length; i++) {
      result[i] = indexKey[i + length];
    }
    return result;
  }

  void println(Result result) {
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

  /**
   * Get a result scanner for sql query.
   * @param indexSql
   * @return
   * @throws Exception
   */
  public IndexResultScanner getScanner(IndexQuerySQL indexSql) throws Exception {
    if (Bytes.compareTo(tableName, indexSql.getTableName()) != 0) {
      throw new IllegalArgumentException("IndexSQL isn't for this table! IndexSQL's table is "
          + Bytes.toString(indexSql.getTableName()) + ", IndexTable's is "
          + Bytes.toString(tableName));
    }
    // check ranges
    indexSql.setColumnInfo(columnTypeMap);
    Range[][] range = indexSql.getRanges();
    if (range == null || range.length == 0) {
      throw new IllegalArgumentException("Scan range is not specified!");
    }

    Set<byte[]> families = indexDesc.getTableDescriptor().getFamiliesKeys();
    for (Range[] temparray : range) {
      for (Range temprange : temparray) {
        if (Bytes.compareTo(temprange.getColumn(), IndexConstants.KEY) == 0) {
          if (temprange.getEndTs() != -1 || temprange.getStartTs() != -1) {
            throw new IllegalArgumentException("key don't support timestamp range!");
          }
          continue;
        }
        byte[][] temp = KeyValue.parseColumn(temprange.getColumn());
        if (temp == null || temp.length == 1 || !families.contains(temp[0])) {
          throw new IllegalArgumentException("Column name is invalid! column name:"
              + Bytes.toString(temprange.getColumn()));
        }
      }
    }

    // check result columns(or families only)
    byte[][] resultColumns = indexSql.getResultColumn();
    if (resultColumns != null && resultColumns.length != 0) {
      for (byte[] tempcolumn : resultColumns) {
        byte[][] temp = KeyValue.parseColumn(tempcolumn);
        if (temp == null || !families.contains(temp[0])) {
          throw new IllegalArgumentException(
              "Column or family name is invalid! Column or family name:"
                  + Bytes.toString(tempcolumn));
        }
      }
    }

    if (chooser == null) {
      chooser = new SimpleIndexChooser(this);
    }

    ArrayList<SingleScanner> scanners = new ArrayList<SingleScanner>();

    for (Range[] temprange : range) {
      List<SingleScanner> scans = this.doGetScanner(temprange, resultColumns);
      scanners.addAll(scans);
    }

    return new IndexResultScanner(scanners, resultColumns, this.resultBufferSize, this.loadFactor);
  }

  /**
   * If provided queries like A1 or A2..or.. An, then each Ai is query independently. Each Ai may
   * span one or more regions. So, for each Ai, several threads are started to query in parallel.
   * Each thread will query one or more regions. Meanwhile, total number of threads for a Ai is no
   * more than {@link #maxScanners}.
   * @param range the restrictions
   * @param resultColumns
   * @return
   * @throws IOException
   * @throws IndexNotExistedException
   */
  private List<SingleScanner> doGetScanner(Range[] range, byte[][] resultColumns)
      throws IOException, IndexNotExistedException {
    // change resultColumns
    if (resultColumns != null) {
      Map<byte[], Set<byte[]>> columnMap = new TreeMap<byte[], Set<byte[]>>(Bytes.BYTES_COMPARATOR);
      for (int i = 0; i < range.length; i++) {
        if (Bytes.compareTo(range[i].getColumn(), IndexConstants.KEY) == 0) {
          continue;
        }
        byte[][] temp = KeyValue.parseColumn(range[i].getColumn());
        Set<byte[]> tempset = columnMap.get(temp[0]);
        if (tempset == null) {
          tempset = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
          columnMap.put(temp[0], tempset);
        }
        tempset.add(temp[1]);
      }

      for (int i = 0; i < resultColumns.length; i++) {
        byte[][] temp = KeyValue.parseColumn(resultColumns[i]);
        if (temp.length == 1) {
          columnMap.put(temp[0], null);
        } else {
          if (columnMap.containsKey(temp[0])) {
            Set<byte[]> tempset = columnMap.get(temp[0]);
            if (tempset != null) {
              tempset.add(temp[1]);
            }
          } else {
            Set<byte[]> tempset = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
            tempset.add(temp[1]);
            columnMap.put(temp[0], tempset);
          }
        }
      }

      ArrayList<byte[]> columnList = new ArrayList<byte[]>();
      for (Map.Entry<byte[], Set<byte[]>> entry : columnMap.entrySet()) {
        if (entry.getValue() == null) {
          columnList.add(entry.getKey());
        } else {
          for (byte[] temp : entry.getValue()) {
            columnList.add(KeyValue.makeColumn(entry.getKey(), temp));
          }
        }
      }

      resultColumns = columnList.toArray(new byte[0][]);
    }

    int flag = Integer.MIN_VALUE;

    if (useIndex) {
      flag = chooser.whichToScan(range, resultColumns);
    } else {
      for (int i = 0; i < range.length; i++) {
        if (Bytes.compareTo(range[i].getColumn(), IndexConstants.KEY) == 0) {
          flag = i;
          break;
        }
      }
    }

    ArrayList<Scan> indexScan = new ArrayList<Scan>();
    // table to scan
    HTable table = null;
    List<HRegionInfo> info = null;

    // start and end key for scan
    byte[] startkey = null;
    byte[] endkey = null;

    // if this index table contains all results that needed
    boolean containAll = true;

    // Contains not range of index column(include key column), choose main
    // data table to scan
    if (flag <= -1) {
      LOG.debug("Not index is used, scan the whole main data table!");

      table = indexTableMaps.get(IndexConstants.KEY);
      info = chooser.getHRegionInfo(IndexConstants.KEY);
      startkey = HConstants.EMPTY_BYTE_ARRAY;
      endkey = HConstants.EMPTY_BYTE_ARRAY;
      containAll = true;
    } else {
      // Choose an index column(include key column) to scan
      LOG.debug("Index " + Bytes.toString(range[flag].getColumn()) + " is used!");
      containAll = chooser.containAllResultColumns(range[flag].getColumn(), resultColumns);
      table = indexTableMaps.get(range[flag].getColumn());
      info = chooser.getHRegionInfo(range[flag].getColumn());

      startkey = range[flag].getStartValue();
      if (startkey == null) {
        startkey = HConstants.EMPTY_BYTE_ARRAY;
      }

      endkey = range[flag].getEndValue();
      if (endkey == null) {
        endkey = HConstants.EMPTY_BYTE_ARRAY;
      }
      // If it is equal, transfer Get to Scan
      if (range[flag].getStartType() == CompareOp.EQUAL) {
        if (Bytes.compareTo(range[flag].getColumn(), IndexConstants.KEY) == 0) {
          endkey = Bytes.add(startkey, IndexConstants.MIN_ROW_KEY);
        } else {
          endkey = Bytes.add(startkey, IndexConstants.MAX_ROW_KEY);
        }
      }

      if (range[flag].getStartValue() != null && range[flag].getStartType() == CompareOp.GREATER) {
        startkey = Bytes.add(startkey, IndexConstants.MIN_ROW_KEY);
      }

      if (range[flag].getEndValue() != null && range[flag].getEndType() == CompareOp.LESS_OR_EQUAL) {
        if (Bytes.compareTo(range[flag].getColumn(), IndexConstants.KEY) == 0) {
          endkey = Bytes.add(endkey, IndexConstants.MIN_ROW_KEY);
        } else {
          endkey = Bytes.add(endkey, IndexConstants.MAX_ROW_KEY);
        }
      }
    }

    LOG.debug("Scan table " + Bytes.toString(table.getTableName()));
    LOG.debug("Table startkey=" + Bytes.toStringBinary(startkey));
    LOG.debug("Table endkey=" + Bytes.toStringBinary(endkey));
    LOG.debug("Contain all result columns?" + containAll);

    // If maxScanThreads is greater than 1, start parallel scan threads
    if (maxScanners > 1) {
      int startRegion = 0;
      int endRegion = info.size() - 1;

      if (flag > -1) {
        for (int m = 0; m < info.size(); m++) {
          HRegionInfo temp = info.get(m);

          if (startkey != null && startkey.length != 0) {
            if (temp.containsRow(startkey)) {
              startRegion = m;
            }
          }
          if (endkey != null && endkey.length != 0) {
            if (temp.containsRow(endkey)) {
              endRegion = m;
            }
          }
        }
      }

      LOG.debug("StartRegion=" + startRegion + ", EndRegion=" + endRegion);

      int distance = endRegion - startRegion + 1;

      LOG.debug("Start parallel scan threads! MaxScanThreads=" + maxScanners
          + ", StartedScanThreads=" + (distance > maxScanners ? maxScanners : distance));

      // If there are too many regions, each thread will scan several
      // regions
      if (distance > maxScanners) {
        float mean = 1.0f * distance / maxScanners;

        // flag for [0,distance-1]
        float startflag = 0;
        float endflag = -1;
        for (int m = 0; m < maxScanners; m++) {
          startflag = endflag + 1;

          if (m != maxScanners - 1) {
            endflag = startflag + mean - 1;
          } else {
            endflag = distance - 1;
          }

          Scan scan = new Scan();

          if (m == 0) {
            scan.setStartRow(startkey);
          } else {
            scan.setStartRow(info.get((int) (startflag) + startRegion).getStartKey());
          }

          if (m == maxScanners - 1) {
            scan.setStopRow(endkey);
          } else {
            scan.setStopRow(info.get((int) (endflag) + startRegion).getEndKey());
          }

          LOG.debug("Scan's StartRow=" + Bytes.toStringBinary(scan.getStartRow()) + " , StopRow="
              + Bytes.toStringBinary(scan.getStopRow()));
          indexScan.add(scan);
        }

      } else {
        for (int m = startRegion; m <= endRegion; m++) {
          Scan scan = new Scan();

          if (m == startRegion) {
            scan.setStartRow(startkey);
          } else {
            scan.setStartRow(info.get(m).getStartKey());
          }

          if (m == endRegion) {
            scan.setStopRow(endkey);
          } else {
            scan.setStopRow(info.get(m).getEndKey());
          }

          LOG.debug("Scan's StartRow=" + Bytes.toStringBinary(scan.getStartRow()) + " , StopRow="
              + Bytes.toStringBinary(scan.getStopRow()));
          indexScan.add(scan);
        }
      }

    }
    // start only one scan threads
    else {
      LOG.debug("Start only one scan thread!");
      Scan scan = new Scan();
      scan.setStartRow(startkey);
      scan.setStopRow(endkey);
      LOG.debug("Scan's StartRow=" + Bytes.toStringBinary(scan.getStartRow()) + " , StopRow="
          + Bytes.toStringBinary(scan.getStopRow()));
      indexScan.add(scan);
    }

    List<Filter> ftlist = new ArrayList<Filter>();
    for (int i = 0; i < range.length; i++) {
      if (i != flag && Bytes.compareTo(range[i].getColumn(), IndexConstants.KEY) != 0) {
        byte[][] fq = KeyValue.parseColumn(range[i].getColumn());

        if (range[i].getEndTs() != -1 || range[i].getStartTs() != -1) {
          TimeRangeFilter t = new TimeRangeFilter(fq[0], fq[1], range[i].getStartTs(),
              range[i].getEndTs());
          t.setFilterIfMissing(range[i].isFilterIfMissing());
          t.setLatestVersionOnly(range[i].isLatestVersionOnly());
          ftlist.add(t);
        }

        if (range[i].getStartValue() != null) {
          SingleColumnValueFilter f = new SingleColumnValueFilter(fq[0], fq[1],
              range[i].getStartType(), range[i].getStartValue());
          f.setFilterIfMissing(range[i].isFilterIfMissing());
          f.setLatestVersionOnly(range[i].isLatestVersionOnly());
          ftlist.add(f);
        }
        if (range[i].getEndValue() != null) {
          SingleColumnValueFilter f = new SingleColumnValueFilter(fq[0], fq[1],
              range[i].getEndType(), range[i].getEndValue());
          f.setFilterIfMissing(range[i].isFilterIfMissing());
          f.setLatestVersionOnly(range[i].isLatestVersionOnly());
          ftlist.add(f);
        }
      }
    }
    FilterList ft = new FilterList(ftlist);

    // ImpSecIndex or SecIndex which don't contain all result columns
    if (!containAll) {
      List<Filter> templist = new ArrayList<Filter>();

      IndexSpecification indexSpec = indexDesc.getIndexSpecification(range[flag].getColumn());
      if (indexSpec.getIndexType() == IndexType.IMPSECONDARYINDEX
          && indexSpec.getAdditionMap().size() != 0) {
        Map<byte[], Set<byte[]>> map = indexSpec.getAdditionMap();

        for (int i = 0; i < range.length; i++) {
          if (i != flag && Bytes.compareTo(range[i].getColumn(), IndexConstants.KEY) != 0) {
            byte[][] fq = KeyValue.parseColumn(range[i].getColumn());
            if (map.containsKey(fq[0])
                && (map.get(fq[0]) == null || map.get(fq[0]).contains(fq[1]))) {
              if (range[i].getEndTs() != -1 || range[i].getStartTs() != -1) {
                TimeRangeFilter t = new TimeRangeFilter(fq[0], fq[1], range[i].getStartTs(),
                    range[i].getEndTs());
                t.setFilterIfMissing(range[i].isFilterIfMissing());
                t.setLatestVersionOnly(range[i].isLatestVersionOnly());
                ftlist.add(t);
              }
              if (range[i].getStartValue() != null) {
                SingleColumnValueFilter f = new SingleColumnValueFilter(fq[0], fq[1],
                    range[i].getStartType(), range[i].getStartValue());
                f.setFilterIfMissing(range[i].isFilterIfMissing());
                f.setLatestVersionOnly(range[i].isLatestVersionOnly());
                templist.add(f);
              }
              if (range[i].getEndValue() != null) {
                SingleColumnValueFilter f = new SingleColumnValueFilter(fq[0], fq[1],
                    range[i].getEndType(), range[i].getEndValue());
                f.setFilterIfMissing(range[i].isFilterIfMissing());
                f.setLatestVersionOnly(range[i].isLatestVersionOnly());
                templist.add(f);
              }

              for (Scan scan : indexScan) {
                scan.addColumn(fq[0], fq[1]);
              }
            }
          }
        }
      }

      // TODO think about FristKeyOnlyFilter
      Filter f = new KeyOnlyFilter();
      templist.add(f);

      for (Scan scan : indexScan) {
        scan.setFilter(new FilterList(templist));
        scan.setCaching(scannerCaching);
        scan.setMaxVersions(1);
        scan.setCacheBlocks(false);
      }

    } else {
      if (resultColumns != null && resultColumns.length == 1 && flag >= 0
          && Bytes.compareTo(range[flag].getColumn(), resultColumns[0]) == 0) {
        ftlist.add(new FirstKeyOnlyFilter());
      }
      FilterList newft = new FilterList(ftlist);

      for (Scan scan : indexScan) {
        scan.setFilter(newft);
        scan.setCaching(scannerCaching);
        scan.setMaxVersions(1);
        scan.setCacheBlocks(false);
      }

      // TODO maybe need to skip index column
      if (resultColumns != null && resultColumns.length != 0) {
        // add selected columns(between 'select' and 'from') to scan
        for (byte[] column : resultColumns) {
          byte[][] tmp = KeyValue.parseColumn(column);

          if (tmp.length == 1) {
            for (Scan scan : indexScan) {
              scan.addFamily(tmp[0]);
            }
          } else {
            if (flag < 0 || Bytes.compareTo(range[flag].getColumn(), column) != 0) {
              for (Scan scan : indexScan) {
                scan.addColumn(tmp[0], tmp[1]);
              }
            }
          }
        }
      }
    }

    List<SingleScanner> indexScanners = new ArrayList<SingleScanner>();

    if (flag <= -1 || Bytes.compareTo(range[flag].getColumn(), IndexConstants.KEY) == 0) {
      for (Scan tempscan : indexScan) {
        indexScanners.add(new NoIndexSingleScanner(tempscan, resultColumns, table));
      }
    } else {
      for (Scan tempscan : indexScan) {
        indexScanners.add(new IndexSingleScanner(tempscan, range, flag, resultColumns, table,
            indexDesc.getKeyGenerator(), indexDesc.getIndexSpecification(range[flag].getColumn()),
            containAll, tableName, ft, maxGetsPerScanner));
      }
    }

    return indexScanners;
  }

  /**
   * Get all HTables, including main data table and index tables. For main data table, key of this
   * map is "key", for index tables, key is index column, e.g. "family:qualifier".
   * @return
   */
  public Map<byte[], HTable> getIndexTableMaps() {
    return Collections.unmodifiableMap(indexTableMaps);
  }

  public IndexChooser getIndexChooser() throws IOException {
    if (chooser == null) {
      chooser = new SimpleIndexChooser(this);
    }
    return chooser;
  }

  public void setIndexChooser(IndexChooser chooser) {
    this.chooser = chooser;
  }

  /**
   * Returns the maximum size in bytes of the write buffer for this HTable.
   * <p>
   * The default value comes from the configuration parameter {@code hbase.client.write.buffer}.
   * @return The size of the write buffer in bytes.
   */
  public long getWriteBufferSize() {
    return writeBufferSize;
  }

  /**
   * Sets the size of the buffer in bytes.
   * <p>
   * If the new size is less than the current amount of data in the write buffer, the buffer gets
   * flushed.
   * @param writeBufferSize The new write buffer size, in bytes.
   * @throws IOException if a remote or network exception occurs.
   */
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    this.writeBufferSize = writeBufferSize;
    for (HTable table : indexTableMaps.values()) {
      table.setWriteBufferSize(writeBufferSize);
    }
    for (HTable table : cctTableMaps.values()) {
      table.setWriteBufferSize(writeBufferSize);
    }
  }

  /**
   * Gets the number of rows that a scanner will fetch at once.
   * <p>
   * The default value comes from {@code hbase.client.scanner.caching}.
   */
  public int getScannerCaching() {
    return scannerCaching;
  }

  /**
   * Sets the number of rows that a scanner will fetch at once.
   * <p>
   * This will override the value specified by {@code hbase.client.scanner.caching}. Increasing this
   * value will reduce the amount of work needed each time {@code next()} is called on a scanner, at
   * the expense of memory use (since more rows will need to be maintained in memory by the
   * scanners).
   * @param scannerCaching the number of rows a scanner will fetch at once.
   */
  public void setScannerCaching(int scannerCaching) {
    this.scannerCaching = scannerCaching;
    for (HTable table : indexTableMaps.values()) {
      table.setScannerCaching(scannerCaching);
    }
  }

  public boolean isAutoFlush() {
    return autoFlush;
  }

  /**
   * Turns 'auto-flush' on or off.
   * <p>
   * When enabled (default), {@link Put} operations don't get buffered/delayed and are immediately
   * executed. This is slower but safer.
   * <p>
   * Turning this off means that multiple {@link Put}s will be accepted before any RPC is actually
   * sent to do the write operations. If the application dies before pending writes get flushed to
   * HBase, data will be lost. Other side effects may include the fact that the application thinks a
   * {@link Put} was executed successfully whereas it was in fact only buffered and the operation
   * may fail when attempting to flush all pending writes. In that case though, the code will retry
   * the failed {@link Put} upon its next attempt to flush the buffer.
   * @param autoFlush Whether or not to enable 'auto-flush'.
   * @see #flushCommits
   */
  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
    for (HTable table : indexTableMaps.values()) {
      table.setAutoFlush(autoFlush);
    }
    for (HTable table : cctTableMaps.values()) {
      table.setAutoFlush(autoFlush);
    }
  }

  public byte[] getTableName() {
    return this.tableName;
  }

  /**
   * Get IndexTableDescriptor of the table.
   * @return IndexDescriptor of the table
   */
  public IndexTableDescriptor getIndexTableDescriptor() {
    return indexDesc;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Executes all the buffered {@link Put} operations.
   * <p>
   * This method gets called once automatically for every {@link Put} or batch of {@link Put}s (when
   * {@link #batch(List)} is used) when {@link #isAutoFlush()} is {@code true}.
   * @throws IOException if a remote or network exception occurs.
   */
  public void flushCommits() throws IOException {
    for (HTable table : indexTableMaps.values()) {
      table.flushCommits();
    }
    for (HTable table : cctTableMaps.values()) {
      table.flushCommits();
    }
  }

  public void close() throws IOException {
    flushCommits();
  }

  /**
   * Set result buffer size for an IndexResultScanner created later.
   * @param bufferSize
   * @throws IllegalArgumentException -buffer size isn't greater than 0
   */
  public void setResultBufferSize(int bufferSize) {
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("Buffer size should be greater than 0!");
    }
    this.resultBufferSize = bufferSize;
  }

  public int getResultBufferSize() {
    return this.resultBufferSize;
  }

  /**
   * Set load factor for an IndexResultScanner created later.
   * @param factor
   * @throws IllegalArgumentException -buffer load fact isn't greater than 0
   */
  public void setLoadFactor(float factor) {
    if (factor <= 0.0f) {
      throw new IllegalArgumentException("Buffer load factor should be greater than 0!");
    }
    this.loadFactor = factor;
  }

  public float getLoadFactor() {
    return this.loadFactor;
  }

  /**
   * Set max scan threads number for an IndexResultScanner created later.
   * @param maxScanThread
   */
  public void setMaxScanThreads(int maxScanThread) {
    if (maxScanThread <= 0) {
      throw new IllegalArgumentException("Max scan threads number should be greater than 0!");
    }
    this.maxScanners = maxScanThread;
  }

  public int getMaxScanThreads() {
    return this.maxScanners;
  }

  /**
   * Set if you want to use index when scaning.
   * @param useindex
   */
  public void setUseIndex(boolean useindex) {
    this.useIndex = useindex;
  }

  public boolean getUseIndex() {
    return this.useIndex;
  }

  public int getMaxGetsPerScan() {
    return this.maxGetsPerScanner;
  }

  /**
   * Set max get threads for a scanner, it will be used when SecIndex or ImpSecIndex is chose to
   * scan and index table doesn't contain all result columns.
   * @param maxGets
   */
  public void setMaxGetsPerScan(int maxGets) {
    if (maxGets > 0) {
      this.maxGetsPerScanner = maxGets;
    }
  }

  /**
   * Get each column's data type of this table.
   * @return
   */
  public Map<byte[], DataType> getColumnInfoMap() {
    if (this.columnTypeMap == null || this.columnTypeMap.isEmpty()) {
      return null;
    } else {
      return Collections.unmodifiableMap(this.columnTypeMap);
    }
  }

  /**
   * Set each column's data type of this table. The new column type info will only be available in
   * this IndexTable instance. If you want to set column type info which will be available in new
   * created IndexTable instance afterwards, you should use {@link CCIndexAdmin}.
   * @param columnTypes
   */
  public void setColumnInfoMap(Map<byte[], DataType> columnTypes) {
    this.columnTypeMap = columnTypes;
  }

}
