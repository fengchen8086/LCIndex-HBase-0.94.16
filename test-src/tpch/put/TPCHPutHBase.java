package tpch.put;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

// winter test on raw hbase!
public class TPCHPutHBase extends TPCHPutBaseClass {

  public TPCHPutHBase(String confPath, String newAddedFile, String tableName, int recordNumber,
      boolean forceFlush, String loadDataPath, int threadNum, String statFile, int regionNumbers)
      throws IOException {
    super(confPath, newAddedFile, tableName, recordNumber, forceFlush, loadDataPath, threadNum,
        statFile, regionNumbers);
  }

  @Override
  protected void checkTable() throws IOException {
    if (admin.tableExists(tableName)) {
      System.out.println("coffey TPCHPutHBase deleting existing table: " + tableName);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    System.out.println("coffey TPCHPutHBase creating table: " + tableName);
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    HColumnDescriptor collumns = new HColumnDescriptor(FAMILY_NAME);
    tableDesc.addFamily(collumns);
    admin.createTable(tableDesc, splitKeys);
    System.out.println("coffey TPCHPutHBase creating table: " + tableName + " finish");
  }
}
