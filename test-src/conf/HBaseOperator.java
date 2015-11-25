package conf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseOperator {

  private Configuration conf = null;
  private HTable table;
  private HBaseAdmin admin;
  private String tableName;

  /**
   * 初始化配置
   * @throws IOException
   */
  public HBaseOperator(String tableName) throws IOException {
    conf = HbaseConfig.getHHConfig();
    try {
      admin = new HBaseAdmin(conf);
      this.tableName = tableName;
      if (admin.tableExists(tableName)) {
        table = new HTable(conf, tableName);
      } else {
        table = null;
      }
    } catch (MasterNotRunningException e) {
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 创建一张表
   */
  public void creatTable(String[] familys) throws Exception {
    if (admin.tableExists(tableName)) {
      System.out.println("table already exists!");
    } else {
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      for (int i = 0; i < familys.length; i++) {
        tableDesc.addFamily(new HColumnDescriptor(familys[i]));
      }
      admin.createTable(tableDesc);
      System.out.println("create table " + tableName + " ok.");
    }
    if (table != null) table.close();
    table = new HTable(conf, tableName);
  }

  /**
   * 删除表
   */
  public void deleteTable() throws Exception {
    if (!admin.tableExists(tableName)) {
      System.out.println("table " + tableName + " does not exist!");
      return;
    }
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    System.out.println("delete table " + tableName + " ok.");
  }

  /**
   * 插入一行记录
   */
  public void addRecord(String rowKey, String family, String qualifier, String value)
      throws Exception {
    try {
      Put put = new Put(Bytes.toBytes(rowKey));
      put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
      table.put(put);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 删除一行记录
   */
  public void delRecord(String rowKey) throws IOException {
    List<Delete> list = new ArrayList<Delete>();
    Delete del = new Delete(rowKey.getBytes());
    list.add(del);
    table.delete(list);
    System.out.println("del recored " + rowKey + " ok.");
  }

  /**
   * 查找一行记录
   */
  public void getOneRecord(String rowKey) throws IOException {
    Get get = new Get(rowKey.getBytes());
    Result rs = table.get(get);
    for (KeyValue kv : rs.raw()) {
      System.out.print(new String(kv.getRow()) + " ");
      System.out.print(new String(kv.getFamily()) + ":");
      System.out.print(new String(kv.getQualifier()) + " ");
      System.out.print(kv.getTimestamp() + " ");
      System.out.println(new String(kv.getValue()));
    }
  }

  /**
   * 显示所有数据
   */
  public void getAllRecord() {
    try {
      Scan s = new Scan();
      ResultScanner ss = table.getScanner(s);
      for (Result r : ss) {
        for (KeyValue kv : r.raw()) {
          System.out.print(new String(kv.getRow()) + " ");
          System.out.print(new String(kv.getFamily()) + ":");
          System.out.print(new String(kv.getQualifier()) + " ");
          System.out.print(kv.getTimestamp() + " ");
          System.out.println(new String(kv.getValue()));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void close() throws IOException {
    table.close();
    admin.close();
  }

  public void moreWork() throws IOException {
    NavigableMap<HRegionInfo, ServerName> nm = table.getRegionLocations();
    for (Entry<HRegionInfo, ServerName> e : nm.entrySet()) {
      System.out.println(e.getKey() + "-->" + e.getValue());
    }
  }
}
