package tpch.scan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.util.Bytes;

import tpch.put.TPCHConstants;
import doWork.LCCIndexConstant;

public class TPCHScanMain {

  int ROUND = 1;
  final boolean printScanResults = false;
  final int printResultInterval = 10000;
  protected final boolean CAL_LATENCY = true;

  public void work(String confPath, String assignedFile, String testClass, String rangeFilterFile,
      int cacheSize) throws IOException, InterruptedException {
    double totalTime = 0, temp;
    ArrayList<Double> timeList = new ArrayList<Double>();
    List<Range> rangeList = getRangesFromFile(rangeFilterFile);
    for (Range r : rangeList) {
      System.out.println("coffey get range: " + TPCHConstants.printRange(r));
    }
    for (int i = 0; i < ROUND; ++i) {
      temp = runOneTime(confPath, assignedFile, testClass, rangeList, cacheSize);
      totalTime += temp;
      timeList.add(temp);
      if (ROUND > 1) {
        Thread.sleep(TPCHConstants.ROUND_SLEEP_TIME);
      }
    }
    System.out.println("coffey report scan, run " + testClass + ", have run " + ROUND
        + " times, avg: " + totalTime / ROUND);
    System.out.println("coffey reporting scan each time: ");
    for (int i = 0; i < timeList.size(); ++i) {
      System.out.println("coffey report scan round " + i + ": " + timeList.get(i));
    }
  }

  private double runOneTime(String confPath, String assignedFile, String testClass,
      List<Range> rangeList, int cacheSize) throws IOException, InterruptedException {
    TPCHScanBaseClass scanBase = null;
    if ("HBase".equalsIgnoreCase(testClass)) {
      scanBase =
          new TPCHScanHBase(confPath, assignedFile, TPCHConstants.HBASE_TABLE_NAME, rangeList);
    } else if ("IR".equalsIgnoreCase(testClass)) {
      scanBase = new TPCHScanIR(confPath, assignedFile, TPCHConstants.IR_TABLE_NAME, rangeList);
    } else if ("LCC".equalsIgnoreCase(testClass)) {
      scanBase = new TPCHScanLCC(confPath, assignedFile, TPCHConstants.LCC_TABLE_NAME, rangeList);
    } else if ("CC".equalsIgnoreCase(testClass)) {
      scanBase =
          new TPCHScanCCIndex(confPath, assignedFile, TPCHConstants.CCIndex_TABLE_NAME, rangeList);
    } else if ("CM".equalsIgnoreCase(testClass)) {
      scanBase =
          new TPCHScanCMIndex(confPath, assignedFile, TPCHConstants.CMIndex_TABLE_NAME, rangeList);
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
    // Result result = null;
    Result[] results = null;
    long latency = 0, startLatency = 0;
    try {
      while (true) {
        if (CAL_LATENCY) {
          startLatency = System.currentTimeMillis();
        }
        results = scanner.next(cacheSize);
        if (CAL_LATENCY) {
          latency += System.currentTimeMillis() - startLatency;
        }
        if (results == null || results.length == 0) {
          break;
        }
        // scanBase.insertResultForCheck(result);
        if (printScanResults) {
          for (Result result : results)
            scanBase.printString(result);
        }
        // System.out.println("winter finish scan for " + results.length + " records");
        resultNum += results.length;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    long timeCost = System.currentTimeMillis() - start;
    if (resultNum == 0) resultNum = 1;
    double avgLatency = latency * 1.0 / resultNum;
    scanBase.finish();
    System.out.println("coffey report scan " + testClass + ", total cost " + timeCost / 1000.0
        + "s to scan " + resultNum + " results, average latency is: " + avgLatency);
    return timeCost / 1000.0;
  }

  private List<Range> getRangesFromFile(String rangeFile) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(rangeFile));
    String line;
    List<Range> rangeList = new ArrayList<Range>();
    while ((line = br.readLine()) != null) {
      // totalPrice \t >= startValue
      // totalPrice \t < stopValue
      if(line.startsWith("#"))
        continue;
      String[] parts = line.split("\t");
      DataType type = parseType(parts[1]);
      CompareOp firstOp = parseOp(parts[2]);
      byte[] firstValue = LCCIndexConstant.parsingStringToBytesWithType(type, parts[3]);
      byte[] column = Bytes.toBytes(TPCHConstants.FAMILY_NAME + ":" + parts[0]);
      Range r = null;
      switch (firstOp) {
      case EQUAL:
      case NOT_EQUAL:
        r = new Range(column, firstValue, firstOp, null, CompareOp.NO_OP);
        break;
      case LESS:
      case LESS_OR_EQUAL:
        r = new Range(column, null, CompareOp.NO_OP, firstValue, firstOp);
        break;
      case GREATER:
      case GREATER_OR_EQUAL:
        CompareOp secondOp = CompareOp.NO_OP;
        byte[] secondValue = null;
        if (parts.length > 4) {
          secondOp = parseOp(parts[4]);
          secondValue = LCCIndexConstant.parsingStringToBytesWithType(type, parts[5]);
        }
        r = new Range(column, type, firstValue, firstOp, secondValue, secondOp);
        break;
      case NO_OP:
        break;
      }
      if (r == null) {
        br.close();
        throw new IOException("coffey meet empty range for line: " + line);
      }
      rangeList.add(r);
    }
    br.close();
    return rangeList;
  }

  private CompareOp parseOp(String str) {
    if ("==".equalsIgnoreCase(str)) return CompareOp.EQUAL;
    if (">".equalsIgnoreCase(str)) return CompareOp.GREATER;
    if (">=".equalsIgnoreCase(str)) return CompareOp.GREATER_OR_EQUAL;
    if ("<".equalsIgnoreCase(str)) return CompareOp.LESS;
    if ("<=".equalsIgnoreCase(str)) return CompareOp.LESS_OR_EQUAL;
    if ("!=".equalsIgnoreCase(str)) return CompareOp.NOT_EQUAL;
    return CompareOp.NO_OP;
  }

  private DataType parseType(String str) {
    if ("boolean".equalsIgnoreCase(str)) return DataType.BOOLEAN;
    if ("double".equalsIgnoreCase(str)) return DataType.DOUBLE;
    if ("int".equalsIgnoreCase(str)) return DataType.INT;
    if ("LONG".equalsIgnoreCase(str)) return DataType.LONG;
    if ("short".equalsIgnoreCase(str)) return DataType.SHORT;
    if ("string".equalsIgnoreCase(str)) return DataType.STRING;
    return DataType.STRING;
  }

  public static void usage() {
    System.out
        .println("ScanMain configure-file-path user-defined-file testclass(hbase/ir/lcc) scanFilterFile cacheSize(int)");
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 5) {
      usage();
      return;
    }
    new TPCHScanMain().work(args[0], args[1], args[2], args[3], Integer.valueOf(args[4]));
  }
}
