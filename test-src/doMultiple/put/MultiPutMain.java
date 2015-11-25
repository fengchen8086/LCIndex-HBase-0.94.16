package doMultiple.put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import doTest.put.PutTestConstants;

public class MultiPutMain {

  public static class FinishCounter {
    private AtomicInteger counter;

    public FinishCounter(int init) {
      counter = new AtomicInteger(init);
    }

    public synchronized int getCounter() {
      return counter.get();
    }

    public synchronized void addCounterOne() {
      counter.incrementAndGet();
    }
  }

  int ROUND = 1;

  public void work(String confPath, String assignedFile, String testClass, int recordNumber,
      boolean forceFlush, String loadDataPath, int threadNum) throws IOException,
      InterruptedException {
    double totalTime = 0, temp;
    ArrayList<Double> timeList = new ArrayList<Double>();
    for (int i = 0; i < ROUND; ++i) {
      temp =
          runOneTime(confPath, assignedFile, testClass, recordNumber, forceFlush, loadDataPath,
            threadNum);
      totalTime += temp;
      timeList.add(temp);
      if (ROUND > 1) {
        Thread.sleep(PutTestConstants.ROUND_SLEEP_TIME);
      }
    }
    System.out.println("coffey report put, run " + testClass + " for " + recordNumber
        + " records, have run " + ROUND + " times, avg: " + totalTime / ROUND);
    System.out.println("coffey reporting put each time: ");
    for (int i = 0; i < timeList.size(); ++i) {
      System.out.println("coffey report put round " + i + ": " + timeList.get(i));
    }
  }

  private double runOneTime(String confPath, String assignedFile, String testClass,
      int recordNumber, boolean forceFlush, String loadDataPath, int threadNum) throws IOException,
      InterruptedException {
    MultiPutBaseClass multiPutBase = null;
    if ("HBase".equalsIgnoreCase(testClass)) {
      multiPutBase =
          new MultiPutHBase(confPath, assignedFile, PutTestConstants.HBASE_TABLE_NAME,
              recordNumber, forceFlush, loadDataPath, threadNum);
    } else if ("CM".equalsIgnoreCase(testClass)) {
      multiPutBase =
          new MultiPutCMIndex(confPath, assignedFile, PutTestConstants.CMIndex_TABLE_NAME,
              recordNumber, forceFlush, loadDataPath, threadNum);
    } else if ("CC".equalsIgnoreCase(testClass)) {
      multiPutBase =
          new MultiPutCCIndex(confPath, assignedFile, PutTestConstants.CCIndex_TABLE_NAME,
              recordNumber, forceFlush, loadDataPath, threadNum);
    } else if ("IR".equalsIgnoreCase(testClass)) {
      multiPutBase =
          new MultiPutIR(confPath, assignedFile, PutTestConstants.IR_TABLE_NAME, recordNumber,
              forceFlush, loadDataPath, threadNum);
    } else if ("LCC".equalsIgnoreCase(testClass)) {
      multiPutBase =
          new MultiPutLCC(confPath, assignedFile, PutTestConstants.LCC_TABLE_NAME, recordNumber,
              forceFlush, loadDataPath, threadNum);
    }
    if (multiPutBase == null) {
      System.out.println("coffey test class " + testClass + " not supported!");
    }
    multiPutBase.checkTable();
    System.out.println("coffey check data done, next to load and insert Data");
    long start = System.currentTimeMillis();
    // multiPutBase.loadDataFromFile();
    // multiPutBase.insertData();
    multiPutBase.loadAndInsertData();
    multiPutBase.finish();
    double latency = multiPutBase.calLatency();
    long timeCost = System.currentTimeMillis() - start;
    System.out.println("coffey average latency: " + latency);
    return timeCost / 1000.0;
  }

  public static void usage() {
    System.out
        .println("MultiPutMain configure-file-path user-defined-file testclass(hbase/ir/lcc) recordnumber(int) forceflush(true/false) loaddatapath threadNum");
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length == 0) {
      new MultiPutMain().work("/home/winter/softwares/hbase-0.94.16/conf/hbase-site.xml",
        "/home/winter/softwares/hbase-0.94.16/conf/winter-assign", "lcc", 1000, true,
        "/home/winter/softwares/hbase-0.94.16/datafile.dat", 3);
      return;
    }
    if (args.length < 7) {
      usage();
      return;
    }
    new MultiPutMain().work(args[0], args[1], args[2], Integer.valueOf(args[3]),
      Boolean.valueOf(args[4]), args[5], Integer.valueOf(args[6]));
  }
}
