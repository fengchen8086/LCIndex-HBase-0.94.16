package doTest.put;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.util.Bytes;

import doTest.put.PutTestConstants.CF_INFO;
import doWork.LCCIndexConstant;

public abstract class ClassPutBase {

  final String tableName;
  final Configuration conf;
  final HBaseAdmin admin;
  final boolean forceFlush;
  final String FAMILY_NAME = PutTestConstants.FAMILY_NAME;
  final int RECORD_NUMBER;
  final Random random;
  final int PRINT_INTERVAL;
  final int PRINT_TIME = 10;
  final int regionNum = 6;
  // byte[][] splitkeys;
  final String ROWKEY_FORMAT = "%012d";
  // final String SPLITKEY_FORMAT = "";

  Queue<Put> queue;

  public ClassPutBase(String confPath, String newAddedFile, String tableName, int recordNumber,
      boolean forceFlush) throws IOException {
    this.tableName = tableName;
    this.forceFlush = forceFlush;
    this.RECORD_NUMBER = recordNumber;
    conf = HBaseConfiguration.create();
    conf.addResource(confPath);
    admin = new HBaseAdmin(conf);
    queue = new LinkedList<Put>();
    random = new Random();
    PRINT_INTERVAL = RECORD_NUMBER / PRINT_TIME;
    PutTestConstants.parseMannuallyAssignedFile(conf, newAddedFile);

    // splitkeys = new byte[regionNum][];
    // int startKey = 0;
    // for (int i = 0; i < regionNum; i++) {
    // // 0 + i *
    // splitkeys[i] = Bytes.toBytes(String.format(ROWKEY_FORMAT, startKey));
    // startKey += recordNumber / regionNum;
    // System.out.println("coffey init splitkeys[" + i + "] as: " + Bytes.toString(splitkeys[i]));
    // }
  }

  abstract protected void checkTable() throws IOException;

  private void innerGenerateRandomData() {
    int counter = 0;
    for (int i = 0; i < RECORD_NUMBER; ++i, ++counter) {
      Put put = new Put(Bytes.toBytes(String.format(ROWKEY_FORMAT, i)));
      for (CF_INFO ci : PutTestConstants.getCFInfo()) {
        if (ci.type == DataType.STRING && ci.isIndex == false) {
          put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(ci.qualifier),
            Bytes.toBytes(RandomStringUtils.random(PutTestConstants.GENERATED_STRING_LENGTH))); // int
        } else {
          put.add(
            Bytes.toBytes(FAMILY_NAME),
            Bytes.toBytes(ci.qualifier),
            LCCIndexConstant.parsingStringToBytesWithType(ci.type,
              String.valueOf(random.nextInt(RECORD_NUMBER)))); // int
        }
      }
      if (counter == PRINT_INTERVAL) {
        counter = 0;
        System.out.println("coffey generate data " + i + " class: " + this.getClass().getName());
      }
      queue.add(put);
    }
  }

  public void generateData(String fileName) throws IOException {
    if (fileName == null) {
      System.out.println("coffey generate random data");
      innerGenerateRandomData();
      return;
    }
    System.out.println("coffey load data from file: " + fileName);
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String line;
    String[] parts, innerParts;
    int counter = 0;
    while ((line = br.readLine()) != null) {
      parts = line.split("\t");
      Put put = new Put(Bytes.toBytes(parts[0]));
      for (CF_INFO ci : PutTestConstants.getCFInfo()) {
        boolean inserted = false;
        for (int i = 1; i < parts.length; ++i) {
          innerParts = parts[i].split(":");
          // cf:value
          if (ci.qualifier.equalsIgnoreCase(innerParts[0])) {
            put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(ci.qualifier),
              LCCIndexConstant.parsingStringToBytesWithType(ci.type, innerParts[1]));
            inserted = true;
            break;
          }
        }
        if (!inserted) { // this must be random!
          if (ci.type == DataType.STRING) {
            put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(ci.qualifier),
              Bytes.toBytes(RandomStringUtils.random(PutTestConstants.GENERATED_STRING_LENGTH))); // int
          } else {
            System.out.println("error, non-string column must be inserted from file");
          }
        }
      }
      queue.add(put);
      ++counter;
      if (counter == RECORD_NUMBER) {
        System.out.println("coffey reach the max RECORD_NUMBER: " + RECORD_NUMBER);
        break;
      }
    }
    br.close();
    if (counter < RECORD_NUMBER) {
      System.out.println("coffey not reach the RECORD_NUMBER: " + counter + " of " + RECORD_NUMBER);
      throw new IOException("coffey not reach the RECORD_NUMBER: " + counter + " of "
          + RECORD_NUMBER);
    }
  }

  public void insertData() throws IOException {
    HTable table = new HTable(conf, tableName);
    int counter = 0, doneSize = 0;
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    while (!queue.isEmpty()) {
      table.put(queue.poll());
      if (counter == PRINT_INTERVAL) {
        counter = 0;
        System.out.println("coffey insert data " + doneSize + " class: "
            + this.getClass().getName() + ", time: " + dateFormat.format(new Date()));
      }
      ++counter;
      ++doneSize;
    }
    table.close();
    System.out.println("coffey totally insert " + doneSize + " records");
  }

  public void finish() throws IOException, InterruptedException {
    if (forceFlush) {
      admin.flush(tableName);
    }
  }
}
