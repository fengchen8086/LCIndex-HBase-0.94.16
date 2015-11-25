package doTest.lcc;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import doWork.LCCIndexConstant;

public class LCCPutData {
  Configuration conf;
  HBaseAdmin admin;

  private final String tableName = LCCIndexConstant.TEST_LCCINDEX_TABLE_NAME;
  private Random random = new Random();

  public LCCPutData(String resourcePath) throws MasterNotRunningException,
      ZooKeeperConnectionException {
    conf = HBaseConfiguration.create();
    if (resourcePath != null) {
      conf.addResource(resourcePath);
    }
    admin = new HBaseAdmin(conf);
  }

  private void putData(int recordsNumber) throws IOException {
    HTable table = new HTable(conf, tableName);
    int indexMaxValue = recordsNumber;
    for (int i = 0; i < recordsNumber; ++i) {
      Put put = new Put(Bytes.toBytes(String.format("%04d", i)));
      // put.add(Bytes.toBytes("f"), Bytes.toBytes("A"),
      // Bytes.toBytes(Double.valueOf(RecordNumber - i))); // double
      // put.add(Bytes.toBytes("f"), Bytes.toBytes("B"),
      // Bytes.toBytes(Integer.valueOf(i))); // string
      // put.add(Bytes.toBytes("f"), Bytes.toBytes("Info"), Bytes.toBytes(String.valueOf(i)));
      put.add(Bytes.toBytes("f"), Bytes.toBytes("A"),
        Bytes.toBytes(Double.valueOf(random.nextInt(recordsNumber)))); // double
      put.add(Bytes.toBytes("f"), Bytes.toBytes("B"),
        Bytes.toBytes(Integer.valueOf(random.nextInt(recordsNumber)))); // string
      put.add(Bytes.toBytes("f"), Bytes.toBytes("Info"),
        Bytes.toBytes(String.valueOf(random.nextInt(recordsNumber)))); // string
      table.put(put);
    }
    table.close();
    System.out.println("putData() finish");
  }

  public void work(int recordsNumber, boolean forceFlush) throws Exception {
    putData(recordsNumber);
    if (forceFlush) {
      admin.flush(tableName);
    }
  }

  public static void main(String[] args) throws Exception {
    new LCCPutData(null).work(10, false);
    System.out.println("All Finished!");
  }
}
