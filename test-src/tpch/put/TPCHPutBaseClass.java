package tpch.put;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import doMultiple.put.MultiPutMain.FinishCounter;
import doWork.LCCIndexConstant;

public abstract class TPCHPutBaseClass {
  enum RunningStatus {
    PREPARE, LOAD_DATA, LOAD_DATA_DONE, INSERT_DATA_DONE, FINISHED
  }

  // hbase operation
  final String tableName;
  final Configuration conf;
  final HBaseAdmin admin;
  final boolean forceFlush;
  final String FAMILY_NAME = TPCHConstants.FAMILY_NAME;

  // data generate and table control
  final int TOTAL_RECORD_NUMBER;
  protected final byte[][] splitKeys;
  final String ROWKEY_FORMAT = "%012d";

  // multiple thread
  final int threadNum;
  final String loadDataPath;
  final int SLEEP_TIME = 1000;
  final String statFile;
  boolean[] threadFinishMark;
  double[] threadLatency;
  AtomicLong maxLatency = new AtomicLong(0);
  public boolean putFinished = false;
  FinishCounter finishCounter = new FinishCounter(0);
  RunnableDataLoader[] loaders;
  RunnableDataInserter[] inserters;

  public TPCHPutBaseClass(String confPath, String newAddedFile, String tableName, int recordNumber,
      boolean forceFlush, String loadDataPath, int threadNum, String statFile, int regionNumbers)
      throws IOException {
    this.tableName = tableName;
    this.forceFlush = forceFlush;
    this.TOTAL_RECORD_NUMBER = recordNumber;
    this.threadNum = threadNum;
    this.loadDataPath = loadDataPath;
    this.statFile = statFile;
    splitKeys = parseSplitkKeys(regionNumbers, LCCIndexConstant.ROWKEY_RANGE);
    conf = HBaseConfiguration.create();
    conf.addResource(confPath);
    TPCHConstants.parseMannuallyAssignedFile(conf, newAddedFile);
    System.out.println("coffey manually set zk " + "hbase.zookeeper.quorum "
        + " hec-14,hec-02,hec-03");
    conf.set("hbase.zookeeper.quorum", "hec-14,hec-02,hec-03");
    admin = new HBaseAdmin(conf);
    loaders = new RunnableDataLoader[threadNum];
    inserters = new RunnableDataInserter[threadNum];
    threadFinishMark = new boolean[threadNum];
    threadLatency = new double[threadNum];
  }

  protected byte[][] parseSplitkKeys(int regionNum, String qualifier) throws IOException {
    byte[][] splitkeys = new byte[regionNum][];
    long minRowkey = 0, maxRowkey = 0;
    File file = new File(statFile);
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    while ((line = br.readLine()) != null) {
      if (line.startsWith(qualifier)) {
        String parts[] = line.split("\t");
        minRowkey = Long.valueOf(parts[3]);
        maxRowkey = Long.valueOf(parts[4]);
      }
    }
    br.close();
    long startKey = minRowkey;
    long interval = (maxRowkey - minRowkey) / regionNum;
    for (int i = 0; i < regionNum; i++) {
      splitkeys[i] = Bytes.toBytes(String.valueOf(startKey));
      startKey += interval;
      System.out.println("coffey init splitkeys[" + i + "] as: " + Bytes.toString(splitkeys[i]));
    }
    return splitkeys;
  }

  public void loadAndInsertData() throws InterruptedException {
    for (int i = 0; i < threadNum; ++i) {
      threadFinishMark[i] = false;
      ConcurrentLinkedQueue<Put> queue = new ConcurrentLinkedQueue<Put>();
      loaders[i] =
          new RunnableDataLoader(i, TOTAL_RECORD_NUMBER / threadNum, TPCHConstants.getDataFileName(
            loadDataPath, i), queue);
      inserters[i] =
          getProperDataInserters(i, TOTAL_RECORD_NUMBER / threadNum, queue, finishCounter);
      new Thread(loaders[i]).start();
      new Thread(inserters[i]).start();
    }
  }

  public boolean hasFinished() {
    return finishCounter.getCounter() == threadNum;
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

  protected long updateMaxLatency(long value) {
    long ret = 0;
    synchronized (maxLatency) {
      ret = maxLatency.get();
      if (ret < value) {
        maxLatency.set(value);
        ret = value;
      }
    }
    return ret;
  }

  public double getMaxLatency() {
    return maxLatency.get() / 1000.0;
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
        try {
          loadData();
        } catch (ParseException e) {
          e.printStackTrace();
        }
        threadFinishMark[id] = true;
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    private void loadData() throws IOException, InterruptedException, ParseException {
      System.out.println("coffey thread " + id + " load data from file: " + dataFileName);
      BufferedReader br = new BufferedReader(new FileReader(dataFileName));
      String line;
      int counter = 0;
      TPCHRecord record = null;
      while ((line = br.readLine()) != null) {
        record = new TPCHRecord(line);
        Put put = record.getPut(FAMILY_NAME);
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
    protected long innerMaxLatency = 0;
    protected final boolean CAL_LATENCY = true;
    ConcurrentLinkedQueue<Put> queue;
    protected final int PRINT_INTERVAL;
    protected int PRINT_TIME = 10;
    protected final int eachRecordNumber;
    protected final int id;
    protected final int SLEEP_INTERVAL = 100;
    FinishCounter fc;

    // status from LOAD_DATA to INSERT_DATA_DONE
    public RunnableDataInserter(int id, int recordNumber, ConcurrentLinkedQueue<Put> queue,
        FinishCounter fc) {
      this.id = id;
      eachRecordNumber = recordNumber;
      if (recordNumber >= 10000 * 10000) {
        PRINT_TIME = 500;
      } else if (recordNumber >= 1000 * 10000) {
        PRINT_TIME = 100;
      } else if (recordNumber >= 200 * 10000) {
        PRINT_TIME = 20;
      }
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
      while (true) {
        // both has done,
        if (threadFinishMark[id] == true && queue.isEmpty()) {
          break;
        } else if (queue.isEmpty()) {
          Thread.sleep(SLEEP_INTERVAL);
          continue;
        }
        if (CAL_LATENCY) {
          start = System.currentTimeMillis();
        }
        table.put(queue.poll());
        if (CAL_LATENCY) {
          long tem = System.currentTimeMillis() - start;
          latency += tem;
          if (tem > innerMaxLatency) {
            innerMaxLatency = updateMaxLatency(tem);
          }
        }
        if (counter == PRINT_INTERVAL) {
          counter = 0;
          System.out.println("coffey thread " + id + " insert data " + doneSize + " class: "
              + this.getClass().getName() + ", time: " + dateFormat.format(new Date()));
        }
        ++counter;
        ++doneSize;
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
