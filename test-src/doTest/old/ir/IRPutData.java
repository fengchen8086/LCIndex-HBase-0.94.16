package doTest.old.ir;

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

public class IRPutData {
  Configuration conf;
  HBaseAdmin admin;

  private final String tableName = LCCIndexConstant.TEST_IRINDEX_TABLE_NAME;
  private final int RecordNumber = 1000;
  private final int IndexMaxValue = RecordNumber; // one - to - one
  private Random random = new Random();

  public IRPutData() throws MasterNotRunningException, ZooKeeperConnectionException {
    conf = HBaseConfiguration.create();
    admin = new HBaseAdmin(conf);
  }

  public void init() throws IOException {
    System.out.println("start init");
  }

  private void putData() throws IOException {
    HTable table = new HTable(conf, tableName);
    for (int i = 0; i < RecordNumber; ++i) {
      Put put = new Put(Bytes.toBytes(String.format("%03d", i)));
      put.add(Bytes.toBytes("f"), Bytes.toBytes("A"), Bytes.toBytes(Double.valueOf(i + 1))); // double
      put.add(Bytes.toBytes("f"), Bytes.toBytes("B"), Bytes.toBytes(Integer.valueOf(i + 2))); // string
      put.add(Bytes.toBytes("f"), Bytes.toBytes("Info"), Bytes.toBytes(String.valueOf(i + 3))); // string
      // put.add(Bytes.toBytes("f"), Bytes.toBytes("A"),
      // Bytes.toBytes(Double.valueOf(random.nextInt(IndexMaxValue)))); // double
      // put.add(Bytes.toBytes("f"), Bytes.toBytes("B"),
      // Bytes.toBytes(Integer.valueOf(random.nextInt(IndexMaxValue)))); // string
      // put.add(Bytes.toBytes("f"), Bytes.toBytes("Info"),
      // Bytes.toBytes(String.valueOf(random.nextInt(IndexMaxValue)))); // string
      table.put(put);
    }
    table.close();
    System.out.println("putData() finish");
  }

  public void work(boolean flush) throws Exception {
    init();
    putData();
    if (flush) {
      admin.flush(tableName);
    }
  }

  public static void main(String[] args) throws Exception {
    new IRPutData().work(false);
    System.out.println("All Finished!");
  }
}
