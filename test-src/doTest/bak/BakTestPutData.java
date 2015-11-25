package doTest.bak;

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

public class BakTestPutData {
  Configuration conf;
  HBaseAdmin admin;

  private final String tableName = "base";
  private final int RecordNumber = 1000;
  private final int IndexMaxValue = RecordNumber; // one - to - one 
  private Random random = new Random();

  public BakTestPutData() throws MasterNotRunningException, ZooKeeperConnectionException {
    conf = HBaseConfiguration.create();
    admin = new HBaseAdmin(conf);
  }

  public void init() throws IOException {
    System.out.println("start init");
  }

  private void putData() throws IOException {
    HTable table = new HTable(conf, tableName);
    for (int i = 0; i < RecordNumber; ++i) {
      Put put = new Put(Bytes.toBytes(String.valueOf(i)));
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c3"),
        Bytes.toBytes(Double.valueOf(random.nextInt(IndexMaxValue)))); // double
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c4"),
        Bytes.toBytes(String.valueOf(random.nextInt(IndexMaxValue)))); // string
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c5"),
        Bytes.toBytes(String.valueOf(random.nextInt(IndexMaxValue)))); // string
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c6"),
        Bytes.toBytes(String.valueOf(random.nextInt(IndexMaxValue)))); // string
      table.put(put);
    }
    table.close();
    System.out.println("putData() finish");
  }

  public void work() throws IOException {
    init();
    putData();
  }

  public static void main(String[] args) throws IOException {
    new BakTestPutData().work();
    System.out.println("All Finished!");
  }
}
