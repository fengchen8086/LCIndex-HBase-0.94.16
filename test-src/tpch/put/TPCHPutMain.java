package tpch.put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class TPCHPutMain {

  public static class FinishCounter {
    private AtomicInteger counter;

    public FinishCounter(int init) {
      counter = new AtomicInteger(init);
    }

    public int getCounter() {
      return counter.get();
    }

    public synchronized void addCounterOne() {
      counter.incrementAndGet();
    }

    public synchronized void setCounter(int value) {
      counter.set(value);
    }
  }

  int ROUND = 1;

  public void work(String confPath, String assignedFile, String testClass, int recordNumber,
      boolean forceFlush, String loadDataPath, int threadNum, String statFile, int regionNumbers)
      throws IOException, InterruptedException {
    double totalTime = 0, temp;
    ArrayList<Double> timeList = new ArrayList<Double>();
    for (int i = 0; i < ROUND; ++i) {
      temp =
          runOneTime(confPath, assignedFile, testClass, recordNumber, forceFlush, loadDataPath,
            threadNum, statFile, regionNumbers);
      totalTime += temp;
      timeList.add(temp);
      if (ROUND > 1) {
        Thread.sleep(TPCHConstants.ROUND_SLEEP_TIME);
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
      int recordNumber, boolean forceFlush, String loadDataPath, int threadNum, String statFile,
      int regionNumbers) throws IOException, InterruptedException {
    TPCHPutBaseClass multiPutBase = null;
    if ("HBase".equalsIgnoreCase(testClass)) {
      multiPutBase =
          new TPCHPutHBase(confPath, assignedFile, TPCHConstants.HBASE_TABLE_NAME, recordNumber,
              forceFlush, loadDataPath, threadNum, statFile, regionNumbers);
    } else if ("CM".equalsIgnoreCase(testClass)) {
      multiPutBase =
          new TPCHPutCMIndex(confPath, assignedFile, TPCHConstants.CMIndex_TABLE_NAME,
              recordNumber, forceFlush, loadDataPath, threadNum, statFile, regionNumbers);
    } else if ("CC".equalsIgnoreCase(testClass)) {
      multiPutBase =
          new TPCHPutCCIndex(confPath, assignedFile, TPCHConstants.CCIndex_TABLE_NAME,
              recordNumber, forceFlush, loadDataPath, threadNum, statFile, regionNumbers);
    } else if ("IR".equalsIgnoreCase(testClass)) {
      multiPutBase =
          new TPCHPutIR(confPath, assignedFile, TPCHConstants.IR_TABLE_NAME, recordNumber,
              forceFlush, loadDataPath, threadNum, statFile, regionNumbers);
    } else if ("LCC".equalsIgnoreCase(testClass)) {
      multiPutBase =
          new TPCHPutLCC(confPath, assignedFile, TPCHConstants.LCC_TABLE_NAME, recordNumber,
              forceFlush, loadDataPath, threadNum, statFile, regionNumbers);
    }
    if (multiPutBase == null) {
      System.out.println("coffey test class " + testClass + " not supported!");
    }
    System.out.println("coffey check table now");
    multiPutBase.checkTable();
    System.out.println("coffey check data done, next to load and insert Data");
    long start = System.currentTimeMillis();
    // multiPutBase.loadDataFromFile();
    // multiPutBase.insertData();
    multiPutBase.loadAndInsertData();
    multiPutBase.finish();
    double latency = multiPutBase.calLatency();
    double maxLatency = multiPutBase.getMaxLatency();
    long timeCost = System.currentTimeMillis() - start;
    System.out.println("coffey average latency: " + latency + ", max latency: " + maxLatency);
    return timeCost / 1000.0;
  }

  public static void usage() {
    System.out
        .println("MultiPutMain configure-file-path user-defined-file testclass(hbase/ir/lcc) recordnumber(int) forceflush(true/false) loaddatapath threadNum");
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 9) {
      usage();
      return;
    }
    new TPCHPutMain().work(args[0], args[1], args[2], Integer.valueOf(args[3]),
      Boolean.valueOf(args[4]), args[5], Integer.valueOf(args[6]), args[7],
      Integer.valueOf(args[8]));
  }
}
