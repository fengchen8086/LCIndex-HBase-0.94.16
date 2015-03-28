package org.apache.hadoop.hbase.index.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.IndexColumnDescriptor;
import org.apache.hadoop.hbase.index.client.IndexDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class IzpPutThroughputTest {
  static String filePath = "hdfs://data12:8020/1e";
  static boolean wal = true;
  static int index = 1;
  BlockingQueue<Put> queue = new LinkedBlockingQueue<Put>(1000000);
  String tableName = "izp30e";
  Configuration conf = HBaseConfiguration.create();
  boolean stop = false;

  FileSystem fs = null;

  Reader reader = null;
  Writer[] writer = null;
  static int writerNum = 100;
  long startTime = 0, lastStartTime = 0;
  long lastWriteCount = 0;
  int minWriteSpeed = Integer.MAX_VALUE;
  int maxWriteSpeed = 0;
  
  int regionNum=500;

  AtomicInteger writeCount = new AtomicInteger(0);

  public IzpPutThroughputTest() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    fs =new Path(filePath).getFileSystem(conf);

    // if(admin.tableExists(tableName)){
    // indexadmin.disableTable(tableName);
    // indexadmin.deleteTable(tableName);
    // }
    if (!admin.tableExists(tableName)) {
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);

      if (index == 1) {
        IndexDescriptor index1 = new IndexDescriptor(Bytes.toBytes("f"), DataType.STRING);
        IndexDescriptor index2 = new IndexDescriptor(Bytes.toBytes("h"), DataType.STRING);
        IndexDescriptor index3 = new IndexDescriptor(Bytes.toBytes("a"), DataType.STRING);
        IndexDescriptor index4 = new IndexDescriptor(Bytes.toBytes("y"), DataType.STRING);
        IndexDescriptor index5 = new IndexDescriptor(Bytes.toBytes("r"), DataType.STRING);
        IndexDescriptor index6 = new IndexDescriptor(Bytes.toBytes("g"), DataType.STRING);
        IndexDescriptor index7 = new IndexDescriptor(Bytes.toBytes("p"), DataType.STRING);
        IndexDescriptor index8 = new IndexDescriptor(Bytes.toBytes("o"), DataType.STRING);
        IndexDescriptor index9 = new IndexDescriptor(Bytes.toBytes("s"), DataType.INT);

        IndexColumnDescriptor family1 = new IndexColumnDescriptor("f");
        family1.addIndex(index1);
        family1.addIndex(index2);
        family1.addIndex(index3);
        family1.addIndex(index4);
        family1.addIndex(index5);
        family1.addIndex(index6);
        family1.addIndex(index7);
        family1.addIndex(index8);
        family1.addIndex(index9);

        HColumnDescriptor family2 = new HColumnDescriptor("q");

        tableDesc.addFamily(family1);
        tableDesc.addFamily(family2);
        
        byte[][] splitkeys=new byte[regionNum][];
        for(int i=0;i<regionNum;i++){
          splitkeys[i]=Bytes.toBytes(String.format("%03d", i));
        }
        admin.createTable(tableDesc, splitkeys);
      } else {
        HColumnDescriptor family = new HColumnDescriptor("f");
        tableDesc.addFamily(family);
        admin.createTable(tableDesc, Bytes.toBytes("000"), Bytes.toBytes("099"), 101);
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

    public void run() {
      stop = false;
      try {
        Path src = new Path(filePath);
        FileStatus[] list = fs.listStatus(src);

        BufferedReader reader = null;
        String line = null, col[] = null;

        for (int i = 0; i < list.length; i++) {
          if(list[i].getPath().getName().startsWith("_")) continue;
          reader = new BufferedReader(new InputStreamReader(fs.open(list[i].getPath())));
          // key ORDERKEY String 0002014061300:00:0000001234567
          // f FLOW_TYPE String
          // h HOST String
          // a AID String
          // y AP_TYPE String
          // r REGION_ID String
          // g AD_GROUP_ID String
          // p AD_PLAN_ID String
          // o OWNER_ID String
          // s SIZE Int
          // w SHOW Int
          // c CLICK Int
          // p PUSH Int
          // i IP Int
          // u UV Int
          // c COST Double
          while ((line = reader.readLine()) != null && !stop) {
            col = line.split("\t");
            Put put = new Put(Bytes.toBytes(col[0]));
            put.add(Bytes.toBytes("f"), Bytes.toBytes("f"), Bytes.toBytes(col[1])); // string
            put.add(Bytes.toBytes("f"), Bytes.toBytes("h"), Bytes.toBytes(col[2])); // string
            put.add(Bytes.toBytes("f"), Bytes.toBytes("a"), Bytes.toBytes(col[3])); // string
            put.add(Bytes.toBytes("f"), Bytes.toBytes("y"), Bytes.toBytes(col[4])); // string
            put.add(Bytes.toBytes("f"), Bytes.toBytes("r"), Bytes.toBytes(col[5])); // string
            put.add(Bytes.toBytes("f"), Bytes.toBytes("g"), Bytes.toBytes(col[6])); // string
            put.add(Bytes.toBytes("f"), Bytes.toBytes("p"), Bytes.toBytes(col[7])); // string
            put.add(Bytes.toBytes("f"), Bytes.toBytes("o"), Bytes.toBytes(col[8])); // string
            put.add(Bytes.toBytes("f"), Bytes.toBytes("s"), Bytes.toBytes(Integer.valueOf(col[9]))); // int
            put.add(Bytes.toBytes("f"), Bytes.toBytes("w"), Bytes.toBytes(Integer.valueOf(col[10]))); // int
            put.add(Bytes.toBytes("f"), Bytes.toBytes("c"), Bytes.toBytes(Integer.valueOf(col[11]))); // int

            put.add(Bytes.toBytes("q"), Bytes.toBytes("p"), Bytes.toBytes(Integer.valueOf(col[12]))); // int
            put.add(Bytes.toBytes("q"), Bytes.toBytes("i"), Bytes.toBytes(Integer.valueOf(col[13]))); // int
            put.add(Bytes.toBytes("q"), Bytes.toBytes("u"), Bytes.toBytes(Integer.valueOf(col[14]))); // int
            put.add(Bytes.toBytes("q"), Bytes.toBytes("c"), Bytes.toBytes(Double.valueOf(col[15]))); // double

            if (!wal) {
              put.setDurability(Durability.SKIP_WAL);
            }
            queue.put(put);
          }

          reader.close();
        }

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

        HTable table = new HTable(conf, tableName);
        for (;;) {
          Put put = queue.poll(1, TimeUnit.SECONDS);
          if (put == null) {
            if (stop) break;
            else continue;
          }
          table.put(put);
          writeCount.incrementAndGet();
        }
        table.close();

      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws IOException {
    try {
      filePath = args[0];
      writerNum = Integer.valueOf(args[1]);
      wal = Boolean.valueOf(args[2]);
      index = Integer.valueOf(args[3]);
    } catch (Exception e) {
      System.out.println("filePath  writerNum  wal index update");
      // return;
    }

    System.out.println("----------------" + filePath);
    System.out.println("----------------" + writerNum);
    System.out.println("----------------" + wal);
    System.out.println("----------------" + index);

    IzpPutThroughputTest test = new IzpPutThroughputTest();
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
