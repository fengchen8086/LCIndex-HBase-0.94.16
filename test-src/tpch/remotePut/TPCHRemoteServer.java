package tpch.remotePut;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import tpch.put.TPCHConstants;
import tpch.remotePut.TPCHRemoteServerThread.RemotePutStatus;

public class TPCHRemoteServer {

  private final int SERVER_PORT = TPCHConstants.REMOTE_SERVER_PORT;
  private List<String> clients;
  private List<String> unFinishedClients;
  private List<String> remoteProcesses;
  private List<TPCHRemoteServerThread> threads;

  private String dataDir; // dataDir = /basicDir/5000-5-10
  private int totalRecordNumber;
  private int printInterval;
  private int eachNumber;

  private final int ReportInterval = 100;
  private final int SLEEP_TIME = 100;

  private void work(String testClass, int recordNum, String dataBaseDir, String statFileName,
      String clientFileName, int threadNumber, boolean forceFlush) throws IOException,
      InterruptedException {
    System.out.println("coffey server " + testClass + " started");
    this.totalRecordNumber = recordNum;
    clients = TPCHRemoteDataGenerator.getClientHosts(clientFileName);
    remoteProcesses = new ArrayList<String>();
    unFinishedClients = new ArrayList<String>();
    for (String client : clients) {
      remoteProcesses.add(client);
    }
    eachNumber = totalRecordNumber / clients.size() / threadNumber;
    printInterval = calInterval();
    dataDir = TPCHConstants.getBaseDir(dataBaseDir, recordNum, clients.size(), threadNumber);
    // start server socket
    resetUnFinishedClients();
    threads = new ArrayList<TPCHRemoteServerThread>();
    // host name and id is given at start
    // still need to reconsider!
    String param =
        dataDir + "\t" + testClass + "\t" + printInterval + "\t" + statFileName + "\t"
            + String.valueOf(forceFlush);
    List<String> unConnectedClients = new ArrayList<String>();
    unConnectedClients.addAll(remoteProcesses);
    ServerSocket ss = new ServerSocket(SERVER_PORT);
    System.out.println("coffey server " + testClass + " start listening");
    System.out.println("coffey server " + testClass + " param: " + param);
    Object syncObj = new Object();
    do {
      Socket s = ss.accept();
      TPCHRemoteServerThread th =
          new TPCHRemoteServerThread(s, unFinishedClients, dataDir, param, threadNumber,
              statFileName, syncObj);
      threads.add(th);
      unConnectedClients.remove(th.getClientName());
      System.out.println("see conncetion from " + th.getClientName() + ", left size: "
          + unConnectedClients.size());
      th.start();
      // start and send parameters to client
    } while (unConnectedClients.size() > 0);
    System.out.println("coffey server " + testClass + " send params now");
    sendParam();
    System.out.println("coffey server " + testClass + " check data ready now");
    checkDataReady();
    System.out.println("coffey server " + testClass + " check remote table");
    checkRemoteTable();
    System.out.println("coffey server " + testClass + " start remote put, on " + clients.size()
        + " clients * " + threadNumber + " proccesses * " + eachNumber + " records each");
    long start = System.currentTimeMillis();
    startRemotePut();
    System.out.println("coffey server " + testClass + " remote flush table");
    setRemoteFlush();
    long totalTime = System.currentTimeMillis() - start;
    System.out.println("coffey server " + testClass + " set remote exit");
    setRemoteStop();
    System.out.println("coffey server " + testClass + " join threads, to wait for finish");
    for (TPCHRemoteServerThread t : threads) {
      t.join();
    }
    ss.close();
    System.out.println("coffey server " + testClass + " finish");
    System.out.println("coffey report put " + testClass + ", cost " + (totalTime) / 1000.0
        + " to write " + recordNum + " on " + clients.size() + " * " + threadNumber + " client");
  }

  private void setAllThreadsStatsu(RemotePutStatus s) {
    for (TPCHRemoteServerThread t : threads) {
      t.setStatus(s);
    }
  }

  private void resetUnFinishedClients() {
    synchronized (unFinishedClients) {
      unFinishedClients.clear();
      unFinishedClients.addAll(remoteProcesses);
    }
  }

  private void waitClientsFinish() throws InterruptedException {
    int meetCount = 0;
    while (true) {
      synchronized (unFinishedClients) {
        if (unFinishedClients.size() == 0) {
          return;
        }
        if (unFinishedClients.size() < 3 && ++meetCount == ReportInterval) {
          meetCount = 0;
          System.out.println("coffey unfinished clients: ");
          for (String s : unFinishedClients) {
            System.out.print(s + ",");
          }
          System.out.println("");
        }
        Thread.sleep(SLEEP_TIME);
      }
    }
  }

  private void sendParam() throws InterruptedException {
    resetUnFinishedClients();
    setAllThreadsStatsu(RemotePutStatus.SEND_PARAM);
    waitClientsFinish();
  }

  private void checkDataReady() throws InterruptedException {
    resetUnFinishedClients();
    setAllThreadsStatsu(RemotePutStatus.CHECK_DATA);
    waitClientsFinish();
  }

  private void checkRemoteTable() throws InterruptedException {
    String clientName = threads.get(0).getClientName();
    synchronized (unFinishedClients) {
      unFinishedClients.clear();
      unFinishedClients.add(clientName);
    }
    threads.get(0).setStatus(RemotePutStatus.CHECK_TABLE);
    waitClientsFinish();
  }

  private void startRemotePut() throws InterruptedException {
    resetUnFinishedClients();
    setAllThreadsStatsu(RemotePutStatus.START_TO_PUT);
    waitClientsFinish();
  }

  private void setRemoteFlush() throws InterruptedException {
    String clientName = threads.get(0).getClientName();
    synchronized (unFinishedClients) {
      unFinishedClients.clear();
      unFinishedClients.add(clientName);
    }
    threads.get(0).setStatus(RemotePutStatus.FLUSH);
    waitClientsFinish();
  }

  private void setRemoteStop() throws InterruptedException {
    resetUnFinishedClients();
    setAllThreadsStatsu(RemotePutStatus.CLOSE);
    waitClientsFinish();
  }

  private int calInterval() {
    int reportInterval = eachNumber / 10;
    if (reportInterval >= 100 * 10000) {
      return 25 * 10000;
    } else if (reportInterval >= 50 * 10000) {
      return 20 * 10000;
    } else if (reportInterval >= 20 * 10000) {
      return 10 * 10000;
    }
    return reportInterval;
  }

  public static void main(String[] args) throws NumberFormatException, IOException,
      InterruptedException {
    if (args.length < 7) {
      usage();
      System.out.println("current:");
      for (String str : args) {
        System.out.println(str);
      }
      return;
    }
    new TPCHRemoteServer().work(args[0], Integer.valueOf(args[1]), args[2], args[3], args[4],
      Integer.valueOf(args[5]), Boolean.valueOf(args[6]));
  }

  public static void usage() {
    System.out
        .println("TPCHRemoteServer workloadType totalRecordNum data_input_dir stat_name client_hosts_file each_client_thread_number forceflush");
  }
}
