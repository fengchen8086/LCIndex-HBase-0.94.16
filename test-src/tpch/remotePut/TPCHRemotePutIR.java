package tpch.remotePut;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.index.client.IndexColumnDescriptor;
import org.apache.hadoop.hbase.index.client.IndexDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import tpch.put.TPCHConstants;
import tpch.put.TPCHConstants.TPCH_CF_INFO;

public class TPCHRemotePutIR extends TPCHRemotePutBaseClass {

  public TPCHRemotePutIR(String confPath, String newAddedFile, String tableName,
      String loadDataDir, int threadNum, String statFilePath, int regionNum, boolean forceFlush,
      int reportInterval, ConcurrentLinkedQueue<String> reportQueue) throws IOException {
    super(confPath, newAddedFile, tableName, loadDataDir, threadNum, statFilePath, regionNum,
        forceFlush, reportInterval, reportQueue);
  }

  @Override
  protected void checkTable() throws IOException {
    if (admin.tableExists(tableName)) {
      System.out.println("coffey TPCHRemotePutBaseClass deleting existing table: " + tableName);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    System.out.println("coffey TPCHRemotePutBaseClass creating ir table: " + tableName);
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    IndexColumnDescriptor family = new IndexColumnDescriptor(FAMILY_NAME);

    List<TPCH_CF_INFO> cfs = TPCHConstants.getCFInfo();
    for (TPCH_CF_INFO ci : cfs) {
      if (ci.isIndex) {
        IndexDescriptor index = new IndexDescriptor(Bytes.toBytes(ci.qualifier), ci.type);
        family.addIndex(index);
        System.out.println("coffey TPCHRemotePutBaseClass has ir index on cf: " + ci.qualifier
            + ", type is: " + ci.type);
      }
    }
    tableDesc.addFamily(family);
    admin.createTable(tableDesc, splitKeys);
    System.out.println("coffey TPCHRemotePutBaseClass creating ir table: " + tableName + " finish");
  }
}
