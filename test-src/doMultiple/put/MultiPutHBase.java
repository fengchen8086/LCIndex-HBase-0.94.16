package doMultiple.put;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

// winter test on raw hbase!
public class MultiPutHBase extends MultiPutBaseClass {

  public MultiPutHBase(String confPath, String newAddedFile, String tableName, int recordNumber,
      boolean forceFlush, String loadDataPath, int threadNum) throws IOException {
    super(confPath, newAddedFile, tableName, recordNumber, forceFlush, loadDataPath, threadNum);
  }

  @Override
  protected void checkTable() throws IOException {
    if (admin.tableExists(tableName)) {
      System.out.println("coffey PutHBase deleting existing table: " + tableName);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    System.out.println("coffey PutHBase creating table: " + tableName);
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    HColumnDescriptor collumns = new HColumnDescriptor(FAMILY_NAME);
    tableDesc.addFamily(collumns);
    admin.createTable(tableDesc, splitkeys);
    System.out.println("coffey PutHBase creating table: " + tableName + " finish");
  }
}
