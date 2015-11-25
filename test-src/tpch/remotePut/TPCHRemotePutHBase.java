package tpch.remotePut;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

// winter test on raw hbase!
public class TPCHRemotePutHBase extends TPCHRemotePutBaseClass {

  public TPCHRemotePutHBase(String confPath, String newAddedFile, String tableName,
      String loadDataDir, int threadNum, String statFilePath, int regionNum, boolean forceFlush,
      int reportInterval, ConcurrentLinkedQueue<String> reportQueue) throws IOException {
    super(confPath, newAddedFile, tableName, loadDataDir, threadNum, statFilePath, regionNum,
        forceFlush, reportInterval, reportQueue);
  }

  @Override
  protected void checkTable() throws IOException {
    if (admin.tableExists(tableName)) {
      printAndAddtoReportQueue("coffey TPCHRemotePutBaseClass deleting existing table: "
          + tableName);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    printAndAddtoReportQueue("coffey TPCHRemotePutBaseClass creating table: " + tableName);
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    HColumnDescriptor collumns = new HColumnDescriptor(FAMILY_NAME);
    tableDesc.addFamily(collumns);
    admin.createTable(tableDesc, splitKeys);
    printAndAddtoReportQueue("coffey TPCHRemotePutBaseClass creating table: " + tableName
        + " finish");
  }
}
