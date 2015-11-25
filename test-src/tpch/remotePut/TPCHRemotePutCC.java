package tpch.remotePut;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ccindex.CCIndexAdmin;
import org.apache.hadoop.hbase.ccindex.IndexExistedException;
import org.apache.hadoop.hbase.ccindex.IndexSpecification;
import org.apache.hadoop.hbase.ccindex.IndexSpecification.IndexType;
import org.apache.hadoop.hbase.ccindex.IndexTable;
import org.apache.hadoop.hbase.ccindex.IndexTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import tpch.put.TPCHConstants;
import tpch.put.TPCHConstants.TPCH_CF_INFO;
import doMultiple.put.MultiPutMain.FinishCounter;

public class TPCHRemotePutCC extends TPCHRemotePutBaseClass {
  CCIndexAdmin indexAdmin;

  public TPCHRemotePutCC(String confPath, String newAddedFile, String tableName,
      String loadDataDir, int threadNum, String statFilePath, int regionNum, boolean forceFlush,
      int reportInterval, ConcurrentLinkedQueue<String> reportQueue) throws IOException {
    super(confPath, newAddedFile, tableName, loadDataDir, threadNum, statFilePath, regionNum,
        forceFlush, reportInterval, reportQueue);
    indexAdmin = new CCIndexAdmin(admin);
  }

  @Override
  protected void checkTable() throws IOException {
    if (indexAdmin.tableExists(tableName)) {
      System.out.println("coffey TPCHRemotePutCC deleting existing table: " + tableName);
      indexAdmin.disableTable(tableName);
      indexAdmin.deleteTable(tableName);
    }
    System.out.println("coffey TPCHRemotePutCC creating table: " + tableName);
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    List<TPCH_CF_INFO> cfs = TPCHConstants.getCFInfo();
    List<IndexSpecification> indexList = new ArrayList<IndexSpecification>();
    Map<byte[], DataType> map = new TreeMap<byte[], DataType>(Bytes.BYTES_COMPARATOR);
    for (TPCH_CF_INFO ci : cfs) {
      if (ci.isIndex) {
        IndexSpecification index = new IndexSpecification(Bytes.toBytes(FAMILY_NAME + ":"
            + ci.qualifier), IndexType.CCINDEX);
        indexList.add(index);
        System.out.println("coffey TPCHRemotePutCC has index on cf: " + ci.qualifier
            + ", type is: " + ci.type);
      }
      map.put(Bytes.toBytes(FAMILY_NAME + ":" + Bytes.toBytes(ci.qualifier)), ci.type);
    }
    tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes(FAMILY_NAME)));
    IndexTableDescriptor indexDesc;
    try {
      indexDesc = new IndexTableDescriptor(tableDesc,
          indexList.toArray(new IndexSpecification[indexList.size()]));
      indexDesc.setSplitKeys(splitKeys);
      indexAdmin.createTable(indexDesc);
    } catch (IndexExistedException e) {
      e.printStackTrace();
    }
    FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IOException("winter want DistributedFileSystem but meet " + fs.getClass().getName());
    }
    FileStatus fileList[] = fs.listStatus(new Path("/hbase"));
    for (FileStatus status : fileList) {
      System.out.println("coffey see dir name: " + status.getPath());
      if (status.getPath().getName().endsWith(Bytes.toString(IndexTable.CCT_FIX))) {
        System.out.println("coffey ccindex meet cct dir: " + status.getPath()
            + ", set replication to 3");
        fs.setReplication(status.getPath(), (short) 3);
      }
    }
    System.out.println("coffey TPCHRemotePutCC creating table: " + tableName + " finish");
  }

  @Override
  public void flush() throws IOException, InterruptedException {
    if (forceFlush) {
      indexAdmin.flushAll(Bytes.toBytes(tableName));
    }
  }

  @Override
  protected RunnableDataInserter getProperDataInserters(int id, int recordNumber,
      ConcurrentLinkedQueue<Put> queue, FinishCounter fc) {
    return new RunnableCCIndexInserter(id, recordNumber, queue, fc);
  }

  class RunnableCCIndexInserter extends RunnableDataInserter {
    public RunnableCCIndexInserter(int id, int reportInterval, ConcurrentLinkedQueue<Put> queue,
        FinishCounter fc) {
      super(id, reportInterval, queue, fc);
    }

    @Override
    public void insertData() throws IOException, InterruptedException {
      IndexTable indexTable = new IndexTable(conf, tableName);
      DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      int counter = 0;
      long start = 0;
      while (true) {
        if (queue.isEmpty()) {
          if (threadFinishMark[id]) {
            break;
          } else {
            Thread.sleep(SLEEP_INTERVAL);
            continue;
          }
        }
        if (CAL_LATENCY) {
          start = System.currentTimeMillis();
        }
        indexTable.put(queue.poll(), false);
        if (CAL_LATENCY) {
          updateLatency(System.currentTimeMillis() - start);
        }
        if (counter == PRINT_INTERVAL) {
          counter = 0;
          System.out.println("coffey cc thread " + id + " insert data " + doneSize + " class: "
              + this.getClass().getName() + ", time: " + dateFormat.format(new Date()));
        }
        ++counter;
        ++doneSize;
      }
      indexTable.close();
      System.out.println("coffey cc thread " + id + " totally insert " + doneSize + " records");
      synchronized (syncBoxObj) {
        totalDoneSize += doneSize;
      }
    }
  }
}
