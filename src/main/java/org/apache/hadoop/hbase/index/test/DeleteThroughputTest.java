package org.apache.hadoop.hbase.index.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.IndexColumnDescriptor;
import org.apache.hadoop.hbase.index.client.IndexDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteThroughputTest {
  static String filePath = "/opt/tpch-test-data/large/xaa";
  static boolean wal = true;
  static int index = 2;

  BlockingQueue<Delete> queue = new LinkedBlockingQueue<Delete>(1000000);
  String tableName = "orders";
  Configuration conf = HBaseConfiguration.create();
  boolean stop = false;

  Reader reader = null;
  Writer[] writer = null;
  static int writerNum = 100;
  long startTime = 0, lastStartTime = 0;
  long lastWriteCount = 0;
  int minWriteSpeed = Integer.MAX_VALUE;
  int maxWriteSpeed = 0;

  AtomicInteger writeCount = new AtomicInteger(0);

  public DeleteThroughputTest() throws IOException, IndexExistedException {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);

    CCIndexAdmin indexadmin = new CCIndexAdmin(admin);
    if (admin.tableExists(tableName)) {
      indexadmin.disableTable(tableName);
      indexadmin.deleteTable(tableName);
    }
    if (!admin.tableExists(tableName)) {
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

    }

    admin.close();
  }

  public void start() throws IOException {
    stop = false;
    reader = new Reader();
    reader.setName("Reader");
    reader.start();

    while (this.queue.size() < 500000) {
      System.out.println("queue:" + queue.size() + ", wait for 1 second...");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    writer = new Writer[writerNum];
    for (int i = 0; i < writer.length; i++) {
      writer[i] = new Writer();
      writer[i].setName("Writer-" + i);
      writer[i].start();
    }

    startTime = System.currentTimeMillis();
    lastStartTime = startTime;
  }

  public void stop() {
    stop = true;
    try {
      reader.join();
      for (int i = 0; i < writer.length; i++) {
        writer[i].join();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void report() {
    long curTime = System.currentTimeMillis();
    if (curTime - startTime < 10) return;

    long curWriteCount = writeCount.intValue();
    long curQueueSize = queue.size();

    int avgWriteSpeed = (int) (curWriteCount * 1000.0 / (curTime - startTime));
    int lastWriteSpeed =
        (int) ((curWriteCount - lastWriteCount) * 1000.0 / (curTime - lastStartTime));
    minWriteSpeed = lastWriteSpeed < minWriteSpeed ? lastWriteSpeed : minWriteSpeed;
    maxWriteSpeed = lastWriteSpeed > maxWriteSpeed ? lastWriteSpeed : maxWriteSpeed;

    lastWriteCount = curWriteCount;
    lastStartTime = curTime;

    System.out.println("time=" + ((curTime - startTime) / 1000) + ",write=" + curWriteCount
        + ", queue=" + curQueueSize + ", avg=" + avgWriteSpeed + ", latest=" + lastWriteSpeed
        + ", min=" + minWriteSpeed + ", max=" + maxWriteSpeed);
  }

  class Reader extends Thread {

    private byte[] reverse(byte[] b) {
      for (int i = 0, j = b.length - 1; i < j; i++, j--) {
        byte tmp = b[i];
        b[i] = b[j];
        b[j] = tmp;
      }
      return b;
    }

    public void run() {
      stop = false;
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
        while ((line = reader.readLine()) != null && !stop) {
          col = line.split("\\|");
          Delete delete = new Delete(reverse(Bytes.toBytes(col[0])));

          if (!wal) {
            delete.setDurability(Durability.SKIP_WAL);
          }
          queue.put(delete);
        }

        reader.close();

      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      stop = true;
    }

  }

  class Writer extends Thread {
    public void run() {
      try {
        if (index == 0 || index == 1) {
          HTable table = new HTable(conf, tableName);
          for (;;) {
            Delete delete = queue.poll(1, TimeUnit.SECONDS);
            if (delete == null) {
              if (stop) break;
              else continue;
            }
            table.delete(delete);
            writeCount.incrementAndGet();
          }
          table.close();
        } else {
          IndexTable table = new IndexTable(conf, tableName);
          for (;;) {
            Delete delete = queue.poll(1, TimeUnit.SECONDS);
            if (delete == null) {
              if (stop) break;
              else continue;
            }
            table.delete(delete);
            writeCount.incrementAndGet();
          }
          table.close();
        }

      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws IOException, IndexExistedException {
    try {
      filePath = args[0];
      writerNum = Integer.valueOf(args[1]);
      wal = Boolean.valueOf(args[2]);
      index = Integer.valueOf(args[3]);
    } catch (Exception e) {
      System.out.println("filePath  writerNum  wal index");
      // return;
    }

    System.out.println("----------------" + filePath);
    System.out.println("----------------" + writerNum);
    System.out.println("----------------" + wal);
    System.out.println("----------------" + index);

    DeleteThroughputTest test = new DeleteThroughputTest();
    if (true) return;

    test.start();
    while (!test.stop || !test.queue.isEmpty()) {
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
