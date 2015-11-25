package aid;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutData {

  private Configuration conf;
  private HBaseAdmin admin;
  private HTable table;
  final String tableName = "scores";
  final String[] familys = { "f1", "f2", "f3" };
  final int TestRecordNumber = 1000;
  final int ReportCount = TestRecordNumber / 20;

  public PutData(String confFilePath) throws IOException {
    conf = HBaseConfiguration.create();
    conf.addResource(confFilePath);
    admin = new HBaseAdmin(conf);
  }

  public void work() throws IOException {
    checkTable();
    insertData();
  }

  private void checkTable() throws IOException {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    for (int i = 0; i < familys.length; i++) {
      tableDesc.addFamily(new HColumnDescriptor(familys[i]));
    }
    admin.createTable(tableDesc);
    table = new HTable(conf, tableName);
  }

  public void insertData() throws IOException {
    int i, counter = 0;
    int interval = 2, intervalCounter = 0;
    for (i = 0; i < TestRecordNumber; ++i, ++counter) {
      if (counter >= ReportCount) {
        counter = 0;
        System.out.println("writing record " + i);
      }
      addRecord(String.valueOf(i), "f1", "", String.valueOf(i));
      if (intervalCounter == interval) {
        addRecord(String.valueOf(i), "f2", "", String.valueOf(i));
        intervalCounter = 0;
      }
    }
  }

  public void addRecord(String rowKey, String family, String qualifier, String value)
      throws IOException {
    Put put = new Put(Bytes.toBytes(rowKey));
    put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    table.put(put);
  }

  public static void main(String[] args) throws IOException {
    // new PutData();
    if (args.length != 1) {
      System.out.println("need to indicate the location of hbase-site.xml");
      return;
    }
    new PutData(args[0]).work();
    System.out.println("put common data all done");
  }
}
