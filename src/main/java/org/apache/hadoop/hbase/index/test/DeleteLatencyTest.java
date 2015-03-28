package org.apache.hadoop.hbase.index.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ccindex.CCIndexAdmin;
import org.apache.hadoop.hbase.ccindex.IndexExistedException;
import org.apache.hadoop.hbase.ccindex.IndexSpecification;
import org.apache.hadoop.hbase.ccindex.IndexSpecification.IndexType;
import org.apache.hadoop.hbase.ccindex.IndexTable;
import org.apache.hadoop.hbase.ccindex.IndexTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.IndexColumnDescriptor;
import org.apache.hadoop.hbase.index.client.IndexDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteLatencyTest {
  static String filePath = "/opt/tpch-test-data/large/xaa";
  static boolean wal = false;
  static int index = 3;
  static int writeNum = 100000;

  ArrayList<Put> queue = new ArrayList<Put>();
  ArrayList<Delete> delqueue=new ArrayList<Delete>();
  String tableName = "orders";
  Configuration conf = HBaseConfiguration.create();

  Writer writer = null;
  long startTime = 0;
  int writeCount = 0;

  public DeleteLatencyTest() throws IOException, IndexExistedException {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    CCIndexAdmin indexadmin = new CCIndexAdmin(admin);

    if (admin.tableExists(tableName)) {
      indexadmin.disableTable(tableName);
      indexadmin.deleteTable(tableName);
    }

    HTableDescriptor tableDesc = new HTableDescriptor(tableName);

    if (index == 1) {
      IndexDescriptor index1 = new IndexDescriptor(Bytes.toBytes("c3"), DataType.DOUBLE);
      IndexDescriptor index2 = new IndexDescriptor(Bytes.toBytes("c4"), DataType.STRING);
      IndexDescriptor index3 = new IndexDescriptor(Bytes.toBytes("c5"), DataType.STRING);

      IndexColumnDescriptor family = new IndexColumnDescriptor("f");
      family.addIndex(index1);
      family.addIndex(index2);
      family.addIndex(index3);

      tableDesc.addFamily(family);
      admin.createTable(tableDesc, Bytes.toBytes("1"), Bytes.toBytes("9"), 10);
    } else if (index == 2) {
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f")));

      IndexSpecification[] index = new IndexSpecification[3];
      index[0] = new IndexSpecification(Bytes.toBytes("f:c3"));
      index[1] = new IndexSpecification(Bytes.toBytes("f:c4"));
      index[2] = new IndexSpecification(Bytes.toBytes("f:c5"));

      IndexTableDescriptor indexDesc = new IndexTableDescriptor(desc, index);
      indexDesc.setSplitKeys(new byte[][] { Bytes.toBytes("1"), Bytes.toBytes("2"),
          Bytes.toBytes("3"), Bytes.toBytes("4"), Bytes.toBytes("5"), Bytes.toBytes("6"),
          Bytes.toBytes("7"), Bytes.toBytes("8"), Bytes.toBytes("9") });
      indexadmin.createTable(indexDesc);

      Map<byte[], DataType> map = new TreeMap<byte[], DataType>(Bytes.BYTES_COMPARATOR);
      map.put(Bytes.toBytes("f:c1"), DataType.INT);
      map.put(Bytes.toBytes("f:c2"), DataType.STRING);
      map.put(Bytes.toBytes("f:c3"), DataType.DOUBLE);
      map.put(Bytes.toBytes("f:c4"), DataType.STRING);
      map.put(Bytes.toBytes("f:c5"), DataType.STRING);
      map.put(Bytes.toBytes("f:c6"), DataType.STRING);
      map.put(Bytes.toBytes("f:c7"), DataType.INT);
      map.put(Bytes.toBytes("f:c8"), DataType.STRING);

      indexadmin.disableTable(tableName);
      indexadmin.setColumnInfoMap(Bytes.toBytes(tableName), map);
      indexadmin.enableTable(tableName);
    } else if (index == 3) {
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f")));

      IndexSpecification[] index = new IndexSpecification[3];
      index[0] = new IndexSpecification(Bytes.toBytes("f:c3"), IndexType.CCINDEX);
      index[1] = new IndexSpecification(Bytes.toBytes("f:c4"), IndexType.CCINDEX);
      index[2] = new IndexSpecification(Bytes.toBytes("f:c5"), IndexType.CCINDEX);

      IndexTableDescriptor indexDesc = new IndexTableDescriptor(desc, index);
      indexDesc.setSplitKeys(new byte[][] { Bytes.toBytes("1"), Bytes.toBytes("2"),
          Bytes.toBytes("3"), Bytes.toBytes("4"), Bytes.toBytes("5"), Bytes.toBytes("6"),
          Bytes.toBytes("7"), Bytes.toBytes("8"), Bytes.toBytes("9") });

      indexadmin.createTable(indexDesc);
      Map<byte[], DataType> map = new TreeMap<byte[], DataType>(Bytes.BYTES_COMPARATOR);
      map.put(Bytes.toBytes("f:c1"), DataType.INT);
      map.put(Bytes.toBytes("f:c2"), DataType.STRING);
      map.put(Bytes.toBytes("f:c3"), DataType.DOUBLE);
      map.put(Bytes.toBytes("f:c4"), DataType.STRING);
      map.put(Bytes.toBytes("f:c5"), DataType.STRING);
      map.put(Bytes.toBytes("f:c6"), DataType.STRING);
      map.put(Bytes.toBytes("f:c7"), DataType.INT);
      map.put(Bytes.toBytes("f:c8"), DataType.STRING);
      indexadmin.disableTable(tableName);
      indexadmin.setColumnInfoMap(Bytes.toBytes(tableName), map);
      indexadmin.enableTable(tableName);

    } else {
      HColumnDescriptor family = new HColumnDescriptor("f");
      tableDesc.addFamily(family);
      admin.createTable(tableDesc, Bytes.toBytes("1"), Bytes.toBytes("9"), 10);
    }

    admin.close();
  }

  public void start() throws IOException {
    writer = new Writer();
    writer.setName("Writer");
    writer.start();
  }

  public void stop() {
    try {
      writer.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void report() {
    if (writeCount == 0) return;

    System.out.println("time=" + ((System.nanoTime() - startTime) / 1000) + ",write=" + writeCount
        + ", latency=" + ((System.nanoTime() - startTime) / writeCount / 1000));
  }

  class Writer extends Thread {

    private byte[] reverse(byte[] b) {
      for (int i = 0, j = b.length - 1; i < j; i++, j--) {
        byte tmp = b[i];
        b[i] = b[j];
        b[j] = tmp;
      }
      return b;
    }

    public void loadData() {
      try {
        BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
        String line = null, col[] = null;

        // key ORDERKEY Int
        // c1 CUSTKEY Int
        // c2 ORDERSTATUS String
        // c3 TOTALPRICE Double index
        // c4 ORDERDATE String index
        // c5 ORDERPRIORITY String index
        // c6 CLERK String
        // c7 SHIPPRIORITY Int
        // c8 COMMENT String
        while ((line = reader.readLine()) != null) {
          col = line.split("\\|");
          Put put = new Put(reverse(Bytes.toBytes(col[0])));
          put.add(Bytes.toBytes("f"), Bytes.toBytes("c1"), Bytes.toBytes(Integer.valueOf(col[1]))); // int
          put.add(Bytes.toBytes("f"), Bytes.toBytes("c2"), Bytes.toBytes(col[2])); // string
          put.add(Bytes.toBytes("f"), Bytes.toBytes("c3"), Bytes.toBytes(Double.valueOf(col[3]))); // double
          put.add(Bytes.toBytes("f"), Bytes.toBytes("c4"), Bytes.toBytes(col[4])); // string
          put.add(Bytes.toBytes("f"), Bytes.toBytes("c5"), Bytes.toBytes(col[5])); // string
          put.add(Bytes.toBytes("f"), Bytes.toBytes("c6"), Bytes.toBytes(col[6])); // string
          put.add(Bytes.toBytes("f"), Bytes.toBytes("c7"), Bytes.toBytes(Integer.valueOf(col[7]))); // int
          put.add(Bytes.toBytes("f"), Bytes.toBytes("c8"), Bytes.toBytes(col[8])); // string

          Delete delete = new Delete(reverse(Bytes.toBytes(col[0])));
          
          if (!wal) {
            put.setDurability(Durability.SKIP_WAL);
            delete.setDurability(Durability.SKIP_WAL);
          }
          queue.add(put);
          delqueue.add(delete);
          
          if (queue.size() > writeNum) {
            break;
          }
        }

        reader.close();

        System.out.println("queue:"+queue.size());
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    public void run() {
      loadData();
      try {
        long stopTime = 0;
        if (index == 0 || index == 1) {
          HTable table = new HTable(conf, tableName);
          for (Put put : queue) {
            table.put(put);
          }
          HBaseAdmin admin=new HBaseAdmin(conf);
          admin.disableTable(tableName);
          admin.enableTable(tableName);
          admin.close();
          System.out.println("insert completed, start delete");
          
          startTime = System.nanoTime();
          for(Delete delete : delqueue){
            table.delete(delete);
            writeCount++;
          }
          stopTime = System.nanoTime();
          
          table.close();
          
        } else {
          IndexTable table = new IndexTable(conf, tableName);
          for (Put put : queue) {
            table.put(put, false);
          }
          CCIndexAdmin admin=new CCIndexAdmin(conf);
          admin.disableTable(tableName);
          admin.enableTable(tableName);
          System.out.println("insert completed, start delete");
          
          startTime = System.nanoTime();
          for(Delete delete:delqueue){
            table.delete(delete);
            writeCount++;
          }
          stopTime = System.nanoTime();
          table.close();
        }

        System.out.println((stopTime - startTime) / writeCount / 1000);

      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws IOException, IndexExistedException {
    try {
      filePath = args[0];
      writeNum = Integer.valueOf(args[1]);
      wal = Boolean.valueOf(args[2]);
      index = Integer.valueOf(args[3]);
    } catch (Exception e) {
      System.out.println("filePath  writeNum  wal index");
//      return;
    }

    System.out.println("----------------" + filePath);
    System.out.println("----------------" + writeNum);
    System.out.println("----------------" + wal);
    System.out.println("----------------" + index);

    DeleteLatencyTest test = new DeleteLatencyTest();
    test.start();
    while (test.writer.isAlive()) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      test.report();
    }

    test.report();
  }

}
