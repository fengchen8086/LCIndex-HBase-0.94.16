package doMultiple.put;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.util.Bytes;

import doMultiple.MultipleConstants;
import doMultiple.put.MultiPutMain.FinishCounter;
import doTest.put.PutTestConstants;
import doTest.put.PutTestConstants.CF_INFO;
import doWork.LCCIndexConstant;

public abstract class MultiPutBaseClass {
  enum RunningStatus {
    PREPARE, LOAD_DATA, LOAD_DATA_DONE, INSERT_DATA_DONE, FINISHED
  }

  // hbase operation
  final String tableName;
  final Configuration conf;
  final HBaseAdmin admin;
  final boolean forceFlush;
  final String FAMILY_NAME = PutTestConstants.FAMILY_NAME;

  // data generate and table control
  final int TOTAL_RECORD_NUMBER;
  final Random random;
  final int regionNum = 6;
  byte[][] splitkeys;
  final String ROWKEY_FORMAT = "%012d";

  // multiple thread
  // RunnableBasic[] putDataRunners;
  final int threadNum;
  final String loadDataPath;
  final int SLEEP_TIME = 1000;
  boolean[] threadFinishMark;
  double[] threadLatency;

  // final String SPLITKEY_FORMAT = "";
  public MultiPutBaseClass(String confPath, String newAddedFile, String tableName,
      int recordNumber, boolean forceFlush, String loadDataPath, int threadNum) throws IOException {
    this.tableName = tableName;
    this.forceFlush = forceFlush;
    this.TOTAL_RECORD_NUMBER = recordNumber;
    this.threadNum = threadNum;
    this.loadDataPath = loadDataPath;
    conf = HBaseConfiguration.create();
    conf.addResource(confPath);
    admin = new HBaseAdmin(conf);
    random = new Random();
    PutTestConstants.parseMannuallyAssignedFile(conf, newAddedFile);

    splitkeys = new byte[regionNum][];
    int startKey = 0;
    for (int i = 0; i < regionNum; i++) {
      splitkeys[i] = Bytes.toBytes(String.format(ROWKEY_FORMAT, startKey));
      startKey += recordNumber / regionNum;
      System.out.println("coffey init splitkeys[" + i + "] as: " + Bytes.toString(splitkeys[i]));
    }
  }

  public void loadAndInsertData() throws InterruptedException {
    RunnableDataLoader[] loaders = new RunnableDataLoader[threadNum];
    RunnableDataInserter[] inserters = new RunnableDataInserter[threadNum];
    threadFinishMark = new boolean[threadNum];
    threadLatency = new double[threadNum];
    FinishCounter fc = new FinishCounter(0);
    for (int i = 0; i < threadNum; ++i) {
      threadFinishMark[i] = false;
      ConcurrentLinkedQueue<Put> queue = new ConcurrentLinkedQueue<Put>();
      loaders[i] =
          new RunnableDataLoader(i, TOTAL_RECORD_NUMBER / threadNum, MultipleConstants.getFileName(
            loadDataPath, i), queue);
      inserters[i] = getProperDataInserters(i, TOTAL_RECORD_NUMBER / threadNum, queue, fc);
      new Thread(loaders[i]).start();
      new Thread(inserters[i]).start();
    }
    while (fc.getCounter() != threadNum) {
      Thread.sleep(SLEEP_TIME);
    }
  }

  protected RunnableDataInserter getProperDataInserters(int id, int recordNumber,
      ConcurrentLinkedQueue<Put> queue, FinishCounter fc) {
    return new RunnableDataInserter(id, recordNumber, queue, fc);
  }

  public void finish() throws IOException, InterruptedException {
    if (forceFlush) {
      admin.flush(tableName);
    }
  }

  public double calLatency() {
    double d = 0;
    for (int i = 0; i < threadNum; ++i) {
      d += threadLatency[i];
    }
    return d / threadNum;
  }

  abstract protected void checkTable() throws IOException;

  public class RunnableDataLoader implements Runnable {
    protected final String dataFileName;
    protected final int PRINT_INTERVAL;
    protected final int PRINT_TIME = 10;
    protected final int eachRecordNumber;
    protected final int id;
    protected final int LOAD_FILE_SLEEP_INTER_VAL = 5 * 1000;
    protected ConcurrentLinkedQueue<Put> queue;
    private final int MAX_QUEUE_SIZE = 2000;
    private final int BACK_INSERT_QUEUE_SIZE = 1000;

    // status from LOAD_DATA to LOAD_DATA_DONE
    public RunnableDataLoader(int id, int recordNumber, String fileName,
        ConcurrentLinkedQueue<Put> queue) {
      this.id = id;
      dataFileName = fileName;
      eachRecordNumber = recordNumber;
      PRINT_INTERVAL = recordNumber / PRINT_TIME;
      this.queue = queue;
    }

    @Override
    public void run() {
      try {
        loadData();
        threadFinishMark[id] = true;
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    private void loadData() throws IOException, InterruptedException {
      System.out.println("coffey thread " + id + " load data from file: " + dataFileName);
      BufferedReader br = new BufferedReader(new FileReader(dataFileName));
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
              System.out.println("coffey thread " + id
                  + " error, non-string column must be inserted from file");
            }
          }
        }
        queue.add(put);
        ++counter;
        if (counter % PRINT_INTERVAL == 0) {
          System.out.println("coffey thread " + id + " load data into queue size: " + counter
              + " class: " + this.getClass().getName());
        }
        if (counter == eachRecordNumber) {
          System.out.println("coffey thread " + id + " reach the max RECORD_NUMBER: "
              + eachRecordNumber);
          break;
        }
        if (queue.size() > MAX_QUEUE_SIZE) {
          while (queue.size() > BACK_INSERT_QUEUE_SIZE) {
            Thread.sleep(LOAD_FILE_SLEEP_INTER_VAL);
          }
        }
      }
      br.close();
      if (counter < eachRecordNumber) {
        System.out.println("coffey thread " + id + " not reach the RECORD_NUMBER: " + counter
            + " of " + eachRecordNumber);
        throw new IOException("coffey thread " + id + " not reach the RECORD_NUMBER: " + counter
            + " of " + eachRecordNumber);
      }
    }
  }

  public class RunnableDataInserter implements Runnable {
    RunningStatus status;
    protected long latency = 0;
    protected final boolean CAL_LATENCY = true;
    ConcurrentLinkedQueue<Put> queue;
    protected final int PRINT_INTERVAL;
    protected final int PRINT_TIME = 10;
    protected final int eachRecordNumber;
    protected final int id;
    protected final int SLEEP_INTER_VAL = 100;
    FinishCounter fc;

    // status from LOAD_DATA to INSERT_DATA_DONE
    public RunnableDataInserter(int id, int recordNumber, ConcurrentLinkedQueue<Put> queue,
        FinishCounter fc) {
      this.id = id;
      eachRecordNumber = recordNumber;
      PRINT_INTERVAL = recordNumber / PRINT_TIME;
      this.queue = queue;
      this.fc = fc;
    }

    @Override
    public void run() {
      try {
        insertData();
        threadLatency[id] = this.getLatency();
        fc.addCounterOne();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    public void insertData() throws IOException, InterruptedException {
      HTable table = new HTable(conf, tableName);
      int counter = 0, doneSize = 0;
      DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      long start = 0;
      while (threadFinishMark[id] != true) {
        while (!queue.isEmpty()) {
          if (CAL_LATENCY) {
            start = System.currentTimeMillis();
          }
          table.put(queue.poll());
          if (CAL_LATENCY) {
            latency += (System.currentTimeMillis() - start);
          }
          if (counter == PRINT_INTERVAL) {
            counter = 0;
            System.out.println("coffey thread " + id + " insert data " + doneSize + " class: "
                + this.getClass().getName() + ", time: " + dateFormat.format(new Date()));
          }
          ++counter;
          ++doneSize;
        }
        Thread.sleep(SLEEP_INTER_VAL);
      }
      table.close();
      System.out.println("coffey totally insert " + doneSize + " records");
    }

    protected double getLatency() {
      System.out.println("coffey thread " + id + " latency " + latency * 1.0 / eachRecordNumber
          / 1000);
      return latency * 1.0 / eachRecordNumber / 1000;
    }
  }
}
