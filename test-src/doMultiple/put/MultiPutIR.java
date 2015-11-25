package doMultiple.put;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.index.client.IndexColumnDescriptor;
import org.apache.hadoop.hbase.index.client.IndexDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import doTest.put.PutTestConstants;
import doTest.put.PutTestConstants.CF_INFO;

public class MultiPutIR extends MultiPutBaseClass {

  public MultiPutIR(String confPath, String newAddedFile, String tableName, int recordNumber,
      boolean forceFlush, String loadDataPath, int threadNum) throws IOException {
    super(confPath, newAddedFile, tableName, recordNumber, forceFlush, loadDataPath, threadNum);
  }

  @Override
  protected void checkTable() throws IOException {
    if (admin.tableExists(tableName)) {
      System.out.println("coffey PutIR deleting existing table: " + tableName);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    System.out.println("coffey PutIR creating ir table: " + tableName);
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    IndexColumnDescriptor family = new IndexColumnDescriptor(FAMILY_NAME);

    List<CF_INFO> cfs = PutTestConstants.getCFInfo();
    for (CF_INFO ci : cfs) {
      if (ci.isIndex) {
        IndexDescriptor index = new IndexDescriptor(Bytes.toBytes(ci.qualifier), ci.type);
        family.addIndex(index);
        System.out.println("coffey PutIR has ir index on cf: " + ci.qualifier + ", type is: "
            + ci.type);
      }
    }
    tableDesc.addFamily(family);
    admin.createTable(tableDesc, splitkeys);
    System.out.println("coffey PutIR creating ir table: " + tableName + " finish");
  }
}
