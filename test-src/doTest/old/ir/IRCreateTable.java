package doTest.old.ir;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.IndexColumnDescriptor;
import org.apache.hadoop.hbase.index.client.IndexDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import doWork.LCCIndexConstant;

public class IRCreateTable {
  private String tableName = LCCIndexConstant.TEST_IRINDEX_TABLE_NAME;

  private void initIRIndex(HBaseAdmin admin) throws IOException {
    System.out.println("start init IRIndex");
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);

    IndexDescriptor index1 = new IndexDescriptor(Bytes.toBytes("A"), DataType.DOUBLE);
    IndexDescriptor index2 = new IndexDescriptor(Bytes.toBytes("B"), DataType.INT);

    IndexColumnDescriptor family = new IndexColumnDescriptor("f");
    family.addIndex(index1);
    family.addIndex(index2);

    tableDesc.addFamily(family);
    admin.createTable(tableDesc);
  }

  public void init() throws IOException {
    System.out.println("start init");
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);

    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      System.out.println("finish deleting existing table and index tables");
    }

    // for each kind
    initIRIndex(admin);
  }

  public void work() throws IOException {
    init();
  }

  public static void main(String[] args) throws IOException {
    new IRCreateTable().work();
    System.out.println("All Finished!");
  }
}
