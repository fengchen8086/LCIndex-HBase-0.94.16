package doTest.scan;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import doTest.put.PutTestConstants;

public class BakOfScanMain {

  int ROUND = 1;
  final boolean printScanResults = true;
  protected final boolean CAL_LATENCY = true;

  public void work(String confPath, String assignedFile, String testClass, int recordNumber,
      double percentage, String outputDir) throws IOException, InterruptedException {
    double totalTime = 0, temp;
    ArrayList<Double> timeList = new ArrayList<Double>();
    for (int i = 0; i < ROUND; ++i) {
      temp = runOneTime(confPath, assignedFile, testClass, recordNumber, percentage, outputDir);
      totalTime += temp;
      timeList.add(temp);
      if (ROUND > 1) {
        Thread.sleep(PutTestConstants.ROUND_SLEEP_TIME);
        outputDir = null;
      }
    }
    System.out.println("coffey report scan, run " + testClass + " for " + recordNumber
        + " records and pertentage " + percentage + ", have run " + ROUND + " times, avg: "
        + totalTime / ROUND);
    System.out.println("coffey reporting scan each time: ");
    for (int i = 0; i < timeList.size(); ++i) {
      System.out.println("coffey report scan round " + i + ": " + timeList.get(i));
    }
  }

  private double runOneTime(String confPath, String assignedFile, String testClass,
      int recordNumber, double percentage, String outputDir) throws IOException,
      InterruptedException {
    ClassScanBase scanBase = null;
    if ("HBase".equalsIgnoreCase(testClass)) {
      scanBase =
          new ScanHBase(confPath, assignedFile, PutTestConstants.HBASE_TABLE_NAME, recordNumber,
              percentage, outputDir == null ? null
                  : (outputDir + "/hbase-" + recordNumber + "-" + percentage));
    } else if ("IR".equalsIgnoreCase(testClass)) {
      scanBase =
          new ScanIR(confPath, assignedFile, PutTestConstants.IR_TABLE_NAME, recordNumber,
              percentage, outputDir == null ? null
                  : (outputDir + "/ir-" + recordNumber + "-" + percentage));
    } else if ("LCC".equalsIgnoreCase(testClass)) {
      scanBase =
          new ScanLCC(confPath, assignedFile, PutTestConstants.LCC_TABLE_NAME, recordNumber,
              percentage, outputDir == null ? null
                  : (outputDir + "/lcc-" + recordNumber + "-" + percentage));
    } else if ("CC".equalsIgnoreCase(testClass)) {
      scanBase =
          new ScanCCIndex(confPath, assignedFile, PutTestConstants.CCIndex_TABLE_NAME,
              recordNumber, percentage, outputDir == null ? null : (outputDir + "/ccindex-"
                  + recordNumber + "-" + percentage));
    } else if ("CM".equalsIgnoreCase(testClass)) {
      scanBase =
          new ScanCMIndex(confPath, assignedFile, PutTestConstants.CMIndex_TABLE_NAME,
              recordNumber, percentage, outputDir == null ? null : (outputDir + "/cmindex-"
                  + recordNumber + "-" + percentage));
    }
    if (scanBase == null) {
      System.out.println("coffey test scan class " + testClass + " not supported!");
    }
    long start = 0;
    if ("CM".equalsIgnoreCase(testClass)) {
      start = System.currentTimeMillis();
    }
    ResultScanner scanner = scanBase.getScanner();
    if (!("CM".equalsIgnoreCase(testClass))) {
      start = System.currentTimeMillis();
    }
    int resultNum = 0;
    Result result = null;
    long latency = 0, startLatency = 0;
    try {
      while (true) {
        if (CAL_LATENCY) {
          startLatency = System.currentTimeMillis();
        }
        result = scanner.next();
        if (CAL_LATENCY) {
          latency += System.currentTimeMillis() - startLatency;
        }
        if (result == null) {
          break;
        }
        // scanBase.insertResultForCheck(result);
        if (printScanResults) {
          scanBase.printString(result);
        }
        ++resultNum;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    long timeCost = System.currentTimeMillis() - start;
    if (resultNum == 0) resultNum = 1;
    double avgLatency = latency * 1.0 / resultNum;
    // int checkedCount = scanBase.finalCheck();
    scanBase.finish();
    System.out.println("coffey report scan " + testClass + ", total cost " + timeCost / 1000.0
        + "s to scan " + resultNum + " results, average latency is: " + avgLatency);
    return timeCost / 1000.0;
  }

  public static void usage() {
    System.out
        .println("ScanMain configure-file-path user-defined-file testclass(hbase/ir/lcc) recordnumber(int) outputDir percentage(double)");
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 6) {
      usage();
      return;
    }
    new BakOfScanMain().work(args[0], args[1], args[2], Integer.valueOf(args[3]),
      Double.valueOf(args[4]), args[5]);
  }
}
