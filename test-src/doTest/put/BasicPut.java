package doTest.put;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class BasicPut {

  final String tableName = "raw";
  final Configuration conf;
  final HBaseAdmin admin;
  final Random random;
  final int RECORD_NUMBER = 1000;

  public BasicPut() throws IOException {
    conf = HBaseConfiguration.create();
    System.out.println(conf.get("hbase.hregion.memstore.flush.size"));
    conf.addResource("/home/winter/softwares/hbase-0.94.16/conf/hbase-site.xml");
    parseMannuallyAssignedFile(conf, "/home/winter/softwares/hbase-0.94.16/conf/winter-assign");
    System.out.println(conf.get("hbase.hregion.memstore.flush.size"));
    admin = new HBaseAdmin(conf);
    random = new Random();
  }

  public static void main(String[] args) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException, InterruptedException {
    BasicPut bp = new BasicPut();
    bp.work();
  }

  private void work() throws IOException, InterruptedException {
    if (admin.tableExists(tableName)) {
      System.out.println("coffey PutHBase deleting existing table: " + tableName);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    System.out.println("coffey PutHBase creating table: " + tableName);
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    HColumnDescriptor collumns = new HColumnDescriptor("f");
    tableDesc.addFamily(collumns);
    admin.createTable(tableDesc);
    System.out.println("coffey PutHBase creating table: " + tableName + " finish");
    HTable table = new HTable(conf, tableName);
    for (int i = 0; i < RECORD_NUMBER; ++i) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c1"), Bytes.toBytes(i)); // int
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c2"), Bytes.toBytes(i)); // int
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c3"), Bytes.toBytes(i)); // int
      table.put(put);
    }
    table.close();
    admin.flush(tableName);
    System.out.println("done");
  }

  public static void parseMannuallyAssignedFile(Configuration conf, String file) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    String parts[] = null;
    while ((line = br.readLine()) != null) {
      if (line.startsWith("#")) {
        System.out.println("skip line: " + line);
        continue;
      }
      parts = line.split("\t");
      if (parts.length != 3) {
        System.out.println("line error: " + line);
        continue;
      }
      System.out.println("setting line: " + line);
      if ("Long".equalsIgnoreCase(parts[1])) {
        conf.setLong(parts[0], Long.valueOf(parts[2]));
      } else if ("Boolean".equalsIgnoreCase(parts[1])) {
        conf.setBoolean(parts[0], Boolean.valueOf(parts[2]));
      } else if ("Int".equalsIgnoreCase(parts[1])) {
        conf.setInt(parts[0], Integer.valueOf(parts[2]));
      } else if ("String".equalsIgnoreCase(parts[1])) {
        conf.setStrings(parts[0], parts[2]);
      } else if ("Float".equalsIgnoreCase(parts[1])) {
        conf.setFloat(parts[0], Float.valueOf(parts[2]));
      } else {
        System.out.println("line type undefined: " + line);
      }
    }
    br.close();
  }
}
