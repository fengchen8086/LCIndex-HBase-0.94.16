package tpch.remotePut;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import tpch.put.TPCHConstants;
import tpch.put.TPCHRecord;
import doMultiple.put.MultiPutMain.FinishCounter;
import doWork.LCCIndexConstant;

public abstract class TPCHRemotePutBaseClass {
  enum RunningStatus {
    PREPARE, LOAD_DATA, LOAD_DATA_DONE, INSERT_DATA_DONE, FINISHED
  }

  // data generate and table control
  protected final byte[][] splitKeys;
  protected final String ROWKEY_FORMAT = "%012d";
  protected final String PERCENT_FORMAT = "%.3f%%";
  protected final double[] latencyBoxPivots = new double[] { 10, 6.0, 5.5, 5.0, 4.5, 4.0, 3.5, 3,
      2.5, 2, 1.8, 1.5, 1.3, 1.2, 1.1, 1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.08, 0.05,
      0.02, 0.01, 0.0 };
  protected final int[] globalBoxNumber;
  protected final Object syncBoxObj = new Object();

  // hbase operation
  final String tableName;
  final Configuration conf;
  final HBaseAdmin admin;
  final boolean forceFlush;
  final String FAMILY_NAME = TPCHConstants.FAMILY_NAME;

  // multiple thread
  // RunnableBasic[] putDataRunners;
  final int threadNum;
  final String loadDataDir;
  final int SLEEP_TIME = 1000;
  final String statFilePath;
  boolean[] threadFinishMark;
  double[] threadLatency;
  AtomicLong maxLatency = new AtomicLong(0);
  final int reportInterval;
  final Queue<String> reportQueue;
  FinishCounter finishCounter = new FinishCounter(0);
  RunnableDataLoader[] loaders;
  RunnableDataInserter[] inserters;
  int totalDoneSize = 0;

  public TPCHRemotePutBaseClass(String confPath, String newAddedFile, String tableName,
      String loadDataDir, int threadNum, String statFilePath, int regionNum, boolean forceFlush,
      int reportInterval, ConcurrentLinkedQueue<String> reportQueue) throws IOException {
    this.tableName = tableName;
    this.forceFlush = forceFlush;
    this.threadNum = threadNum;
    this.loadDataDir = loadDataDir;
    this.statFilePath = statFilePath;
    this.reportInterval = reportInterval;
    this.reportQueue = reportQueue;
    splitKeys = parseSplitkKeys(regionNum, LCCIndexConstant.ROWKEY_RANGE);
    conf = HBaseConfiguration.create();
    conf.addResource(confPath);
    TPCHConstants.parseMannuallyAssignedFile(conf, newAddedFile);
//    System.out.println("coffey manually set zk " + "hbase.zookeeper.quorum "
//        + " hec-14,hec-02,hec-03");
//    conf.set("hbase.zookeeper.quorum", "hec-14,hec-02,hec-03");
    admin = new HBaseAdmin(conf);
    loaders = new RunnableDataLoader[threadNum];
    inserters = new RunnableDataInserter[threadNum];
    threadFinishMark = new boolean[threadNum];
    threadLatency = new double[threadNum];
    globalBoxNumber = new int[latencyBoxPivots.length];
    for (int i = 0; i < globalBoxNumber.length; ++i) {
      globalBoxNumber[i] = 0;
    }
  }

  protected byte[][] parseSplitkKeys(int regionNum, String qualifier) throws IOException {
    byte[][] splitkeys = new byte[regionNum][];
    long minRowkey = 0, maxRowkey = 0;
    File file = new File(statFilePath);
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
          new RunnableDataLoader(i, reportInterval, TPCHConstants.getDataFileName(loadDataDir, i),
              queue);
      inserters[i] = getProperDataInserters(i, reportInterval, queue, finishCounter);
      new Thread(loaders[i]).start();
      new Thread(inserters[i]).start();
    }
  }

  public boolean hasFinished() {
    return finishCounter.getCounter() == threadNum;
  }

  protected RunnableDataInserter getProperDataInserters(int id, int reportInterval,
      ConcurrentLinkedQueue<Put> queue, FinishCounter fc) {
    return new RunnableDataInserter(id, reportInterval, queue, fc);
  }

  public void flush() throws IOException, InterruptedException {
    if (forceFlush) {
      admin.flush(tableName);
    }
  }

  // 1st
  public double calAvgLatency() {
    double d = 0;
    for (int i = 0; i < threadNum; ++i) {
      d += threadLatency[i];
    }
    return d / threadNum;
  }

  // 2nd
  public double getMaxLatency() {
    return maxLatency.get() / 1000.0;
  }

  // 3rd
  public List<String> calDetailLatency() {
    List<String> latencyString = new ArrayList<String>();
    String prefix = "latency report ";
    int part = 0;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < globalBoxNumber.length; ++i) {
      if (sb.length() > 500) {
        latencyString.add(sb.toString());
        sb = new StringBuilder();
        ++part;
      }
      if (sb.length() == 0) {
        sb.append(prefix).append("part: ").append(part);
      }
      sb.append(", [").append(latencyBoxPivots[i]).append("->")
          .append(String.format(PERCENT_FORMAT, 100.0 * globalBoxNumber[i] / totalDoneSize))
          .append("]");
    }
    latencyString.add(sb.toString());
    return latencyString;
  }

  private long updateMaxLatency(long value) {
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

  abstract protected void checkTable() throws IOException;

  public class RunnableDataLoader implements Runnable {
    protected final String dataFileName;
    protected final int PRINT_INTERVAL;
    protected final int id;
    protected final int LOAD_FILE_SLEEP_INTER_VAL = 5 * 1000;
    protected ConcurrentLinkedQueue<Put> queue;
    private final int MAX_QUEUE_SIZE = 2000;
    private final int BACK_INSERT_QUEUE_SIZE = 1000;

    // status from LOAD_DATA to LOAD_DATA_DONE
    public RunnableDataLoader(int id, int reportInterval, String fileName,
        ConcurrentLinkedQueue<Put> queue) {
      this.id = id;
      dataFileName = fileName;
      PRINT_INTERVAL = reportInterval;
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
      printAndAddtoReportQueue("coffey thread " + id + " load data from file: " + dataFileName);
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
          printAndAddtoReportQueue("coffey thread " + id + " load data into queue size: " + counter
              + " class: " + this.getClass().getName());
        }
        if (queue.size() > MAX_QUEUE_SIZE) {
          while (queue.size() > BACK_INSERT_QUEUE_SIZE) {
            Thread.sleep(LOAD_FILE_SLEEP_INTER_VAL);
          }
        }
      }
      br.close();
      printAndAddtoReportQueue("coffey thread " + id + " totally load " + counter + " records");
    }
  }

  public class RunnableDataInserter implements Runnable {
    RunningStatus status;
    protected long totalLatency = 0;
    protected long innerMaxLatency = 0;
    protected int[] latencyBoxNumbers;
    protected final boolean CAL_LATENCY = true;
    ConcurrentLinkedQueue<Put> queue;
    protected final int PRINT_INTERVAL;
    protected int PRINT_TIME = 10;
    protected final int id;
    protected final int SLEEP_INTERVAL = 100;
    protected int doneSize = 0;
    FinishCounter fc;

    // status from LOAD_DATA to INSERT_DATA_DONE
    public RunnableDataInserter(int id, int reportInterval, ConcurrentLinkedQueue<Put> queue,
        FinishCounter fc) {
      this.id = id;
      PRINT_INTERVAL = reportInterval;
      this.queue = queue;
      this.fc = fc;
      latencyBoxNumbers = new int[latencyBoxPivots.length];
      for (int i = 0; i < latencyBoxNumbers.length; ++i) {
        latencyBoxNumbers[i] = 0;
      }
    }

    @Override
    public void run() {
      try {
        insertData();
        threadLatency[id] = this.getLatency();
        System.out.println("coffey thread " + id + " before sync update data");
        synchronized (syncBoxObj) {
          for (int i = 0; i < latencyBoxNumbers.length; ++i) {
            globalBoxNumber[i] += latencyBoxNumbers[i];
          }
        }
        System.out.println("coffey thread " + id + " before sync update data");
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      fc.addCounterOne();
      printAndAddtoReportQueue("coffey thread " + id + " finish, now fc is: " + fc.getCounter()
          + " total want: " + threadNum);
    }

    public void insertData() throws IOException, InterruptedException {
      HTable table = new HTable(conf, tableName);
      DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      int counter = 0;
      long start = 0;
      while (true) {
        if (queue.isEmpty()) {
          if (threadFinishMark[id]) {
            break;
          } else {
            Thread.sleep(SLEEP_INTERVAL);
            continue;
          }
        }
        if (CAL_LATENCY) {
          start = System.currentTimeMillis();
        }
        table.put(queue.poll());
        if (CAL_LATENCY) {
          updateLatency(System.currentTimeMillis() - start);
        }
        if (counter == PRINT_INTERVAL) {
          counter = 0;
          printAndAddtoReportQueue("coffey thread " + id + " insert data " + doneSize + " class: "
              + this.getClass().getName() + ", time: " + dateFormat.format(new Date()));
        }
        ++counter;
        ++doneSize;
      }
      table.close();
      printAndAddtoReportQueue("coffey totally insert " + doneSize + " records");
      synchronized (syncBoxObj) {
        totalDoneSize += doneSize;
      }
    }

    protected double getLatency() {
      printAndAddtoReportQueue("coffey thread " + id + " latency " + totalLatency * 1.0 / doneSize
          / 1000);
      return totalLatency * 1.0 / doneSize / 1000;
    }

    protected void updateLatency(long timeInMS) {
      totalLatency += timeInMS;
      if (timeInMS > innerMaxLatency) {
        innerMaxLatency = updateMaxLatency(timeInMS);
      }
      for (int i = 0; i < latencyBoxPivots.length; ++i) {
        if (latencyBoxPivots[i] <= (timeInMS / 1000.0)) {
          ++latencyBoxNumbers[i];
          break;
        }
      }
    }
  }

  protected synchronized void printAndAddtoReportQueue(String msg) {
    System.out.println(msg);
    reportQueue.add(msg);
  }
}
