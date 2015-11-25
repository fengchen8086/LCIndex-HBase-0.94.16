package tpch.put;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.index.client.DataType;

import doWork.LCCIndexConstant;

public class TPCHDataGenerator {

  final String GENERATED_FORMAT = "%012d";
  final static int RANGE_PARTS = 1000;

  private void work(int recordNumber, String dataFileName, String targetFilePrefix,
      String targetStatisticFile, int threadNumber) throws IOException {
    System.out.println("coffey generating data files for " + threadNumber + " threads from file: "
        + dataFileName);
    countLinesAndWriteStat(recordNumber, dataFileName, targetStatisticFile);
    final int eachSpan = recordNumber / threadNumber;
    BufferedReader br = new BufferedReader(new FileReader(dataFileName));
    String line, fileName = "";
    int lineCounter = 0, fileCounter = 0;
    BufferedWriter bw = null;
    while ((line = br.readLine()) != null) {
      if (lineCounter == 0) {
        fileName = TPCHConstants.getDataFileName(targetFilePrefix, fileCounter);
        bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)));
      }
      bw.write(line + "\n");
      ++lineCounter;
      if (lineCounter == eachSpan) {
        bw.close();
        System.out.println("winter write into " + fileName + " for " + lineCounter + " records");
        lineCounter = 0;
        ++fileCounter;
        if (fileCounter == threadNumber) break;
      }
    }
    if (lineCounter != 0) {
      System.out.println("winter out of loop write into " + fileName + " for " + lineCounter
          + " records");
      bw.close();
      System.out.println("winter write into " + fileName + " for " + lineCounter + " records");
    }
    br.close();
  }

  public static int countLinesAndWriteStat(int recordNumber, String filename, String statFile)
      throws IOException {
    double maxPrice = Double.MIN_NORMAL, minPrice = Double.MAX_VALUE;
    long maxDate = Long.MIN_VALUE, minDate = Long.MAX_VALUE;
    long maxRowkey = Long.MIN_VALUE, minRowkey = Long.MAX_VALUE;
    Set<String> set = new TreeSet<String>();
    BufferedReader br = new BufferedReader(new FileReader(filename));
    String line;
    int lineCounter = 0;
    while ((line = br.readLine()) != null) {
      TPCHRecord record = null;
      try {
        record = new TPCHRecord(line);
      } catch (ParseException e) {
        e.printStackTrace();
        br.close();
        return -1;
      }
      set.add(record.getPriority());
      if (maxRowkey < Long.valueOf(record.getOrderKey())) maxRowkey =
          Long.valueOf(record.getOrderKey());
      if (minRowkey > Long.valueOf(record.getOrderKey())) minRowkey =
          Long.valueOf(record.getOrderKey());
      if (maxDate < record.getDate()) maxDate = record.getDate();
      if (minDate > record.getDate()) minDate = record.getDate();
      if (maxPrice < record.getTotalPrice()) maxPrice = record.getTotalPrice();
      if (minPrice > record.getTotalPrice()) minPrice = record.getTotalPrice();
      ++lineCounter;
      if (lineCounter == recordNumber) {
        break;
      }
    }
    br.close();
    if (lineCounter < recordNumber) {
      throw new IOException("coffey not enough lines found in file: " + filename + ", want: "
          + recordNumber + ", found: " + lineCounter);
    }
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(statFile)));
    bw.write(LCCIndexConstant.ROWKEY_RANGE + "\t" + DataType.LONG + "\t1000\t" + minRowkey + "\t"
        + maxRowkey + "\n");
    bw.write("t\t" + DataType.DOUBLE + "\t" + RANGE_PARTS + "\t" + minPrice + "\t" + maxPrice
        + "\n");
    bw.write("d\t" + DataType.LONG + "\t" + RANGE_PARTS + "\t" + minDate + "\t" + maxDate + "\n");
    String str = "p\t" + DataType.STRING + "\tset";
    for (String s : set) {
      str = str + "\t" + s;
    }
    bw.write(str);
    bw.write("\n");
    bw.close();
    return lineCounter;
  }

  public static void usage() {
    System.out
        .println("TPCHDataSplitter max_record_number(int) tpc_data_source path_to_write_data path_to_write_range thread_number");
  }

  public static void main(String[] args) throws NumberFormatException, IOException {
    if (args.length < 5) {
      usage();
      System.out.println("current:  ");
      for (String str : args) {
        System.out.println(str);
      }
      return;
    }
    new TPCHDataGenerator().work(Integer.valueOf(args[0]), args[1], args[2], args[3],
      Integer.valueOf(args[4]));
  }
}
