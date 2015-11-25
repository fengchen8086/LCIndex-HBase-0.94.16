package tpch.remotePut;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.index.client.DataType;

import tpch.put.TPCHConstants;
import tpch.put.TPCHRecord;
import doWork.LCCIndexConstant;

public class TPCHRemoteDataGenerator {

  final String GENERATED_FORMAT = "%012d";
  final static int RANGE_PARTS = 1000;

  private void work(int recordNumber, String inputData, String outputDir, String statName,
      String clientFile, int threadNumber, boolean deleteExists) throws IOException {
    List<String> clients = getClientHosts(clientFile);
    // baseDir = /outputDir/50000-5-200000
    String baseDir =
        TPCHConstants.getBaseDir(outputDir, recordNumber, clients.size(), threadNumber);
    // statFile = /outputDir/50000-5-200000/statName
    String statFile = TPCHConstants.getClientDir(baseDir, statName);
    boolean needReGenerate = deleteExists ? deleteExists : needReGene(clients, baseDir, statFile);
    if (!needReGenerate) {
      System.out.println("winter file already exists, skip");
      System.out.println("coffey generating data done files for " + clients.size()
          + " clients, each has " + threadNumber + " threads from file: " + inputData
          + ", stat written into: " + statFile);
      return;
    }
    File dir = new File(baseDir);
    if (dir.exists()) {
      FileUtils.deleteDirectory(dir);
    }
    dir.mkdirs();
    System.out.println("coffey generating data files for " + clients.size() + " clients, each has "
        + threadNumber + " threads from file: " + inputData + ", stat written into: " + statFile);
    countLinesAndWriteStat(recordNumber, inputData, statFile);
    final int eachSpan = recordNumber / clients.size() / threadNumber;
    BufferedReader br = new BufferedReader(new FileReader(inputData));
    String line;
    BufferedWriter bw = null;
    for (String hostName : clients) {
      // clientDir = /outputDir/50000-5-200000/hec-01
      String clientDir = TPCHConstants.getClientDir(baseDir, hostName);
      new File(clientDir).mkdirs();
      for (int i = 0; i < threadNumber; ++i) {
        int finishedLine = 0;
        String fileName = TPCHConstants.getDataFileName(clientDir, i);
        bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)));
        while (finishedLine < eachSpan) {
          line = br.readLine();
          bw.write(line + "\n");
          ++finishedLine;
        }
        bw.close();
        System.out.println("coffey write into " + fileName + " for " + finishedLine + " records");
      }
      System.out.println("coffey finished write " + threadNumber + " files in " + clientDir);
    }
    br.close();
  }

  private boolean needReGene(List<String> clients, String baseDir, String statFile) {
    for (String client : clients) {
      if (!new File(TPCHConstants.getClientDir(baseDir, client)).exists()) return true;
    }
    if (new File(statFile).exists()) return false;
    else return true;
  }

  public static List<String> getClientHosts(String clientFile) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(clientFile));
    List<String> clients = new ArrayList<String>();
    String line;
    while ((line = br.readLine()) != null) {
      if (!line.startsWith("#")) {
        clients.add(line);
      }
    }
    br.close();
    return clients;
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
        .println("TPCHAllRemoteGenerator max_record_number(int) tpc_data_source data_output_dir stat_name client_hosts_file each_client_thread_number delete-if-exist");
  }

  public static void main(String[] args) throws NumberFormatException, IOException {
    if (args.length < 7) {
      usage();
      System.out.println("current:");
      for (String str : args) {
        System.out.println(str);
      }
      return;
    }
    new TPCHRemoteDataGenerator().work(Integer.valueOf(args[0]), args[1], args[2], args[3],
      args[4], Integer.valueOf(args[5]), Boolean.valueOf(args[6]));
  }
}
