package tpch.remotePut;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hbase.util.Bytes;

import tpch.put.TPCHConstants;
import tpch.remotePut.TPCHRemoteServerThread.RemotePutStatus;

public class TPCHRemoteClient {

  private int PORT = TPCHConstants.REMOTE_SERVER_PORT;
  private Socket sock;
  private OutputStream sockOut;
  private InputStream sockIn;

  private final int innerBufferLength = 2048;
  private final int SLEEP_TIME = 100;
  private final int INIT_REGION_NUM = 100;

  private String dataDir;
  private String clientDir; // parse from param from server + thisHost + thisID
  private String clientName;
  private String statFilePath;
  private TPCHRemotePutBaseClass putClass;
  private String testClassName;
  private int threadNum;
  private ConcurrentLinkedQueue<String> reportQueue;
  private String confPath;
  private String additionalConf;
  private int reportInterval;
  private boolean forceFlush;

  public void work(String confPath, String additionalConf, String serverName, String thisHost,
      int threadNumber) throws UnknownHostException, IOException, InterruptedException {
    System.out.println("client connecting to " + serverName + ":" + PORT);
    long start = System.currentTimeMillis();
    while (sock == null) {
      try {
        sock = new Socket(serverName, PORT);
      } catch (Exception e) {
        sock = null;
        Thread.sleep(1000);
      }
      if ((System.currentTimeMillis() - start) > 60 * 1000) {
        break;
      }
    }
    if (sock == null) {
      throw new IOException("oh oh, client " + thisHost + " cannot connect to " + serverName + ":"
          + PORT + "after trying 60 seconds");
    }

    System.out.println("client connect to " + serverName + ":" + PORT + " success, this is: "
        + thisHost + " for " + threadNumber + " threads");
    sockOut = sock.getOutputStream();
    sockIn = sock.getInputStream();
    this.confPath = confPath;
    this.additionalConf = additionalConf;
    this.clientName = thisHost;
    this.threadNum = threadNumber;
    reportQueue = new ConcurrentLinkedQueue<String>();
    sockOut.write(Bytes.toBytes(thisHost));
    while (true) {
      RemotePutStatus status = RemotePutStatus.valueOf(readShortMessage());
      if (status != RemotePutStatus.SAY_HI) {
        System.out.println("client " + clientName + " receive new status: " + status);
      }
      if (status == RemotePutStatus.CLOSE) {
        double avgLatency = putClass.calAvgLatency();
        double maxLatency = putClass.getMaxLatency();
        sockOut.write(Bytes.toBytes("avg latency: " + avgLatency + ", max latency: " + maxLatency));
        readShortMessage();
        List<String> list = putClass.calDetailLatency();
        sockOut.write(Bytes.toBytes(String.valueOf(list.size())));
        readShortMessage();
        for (String str : list) {
          sockOut.write(Bytes.toBytes(str));
          readShortMessage();
        }
        writeNoMeanMessage();
        deinit();
        break;
      }
      switch (status) {
      case SEND_PARAM:
        System.out.println("client " + clientName + " " + status + " doing");
        writeNoMeanMessage();
        parseParams(readShortMessage());
        writeNoMeanMessage();
        System.out.println("client " + clientName + " " + status + " done");
        break;
      case CHECK_DATA:
        boolean ready = checkInputFileRead();
        sockOut.write(Bytes.toBytes(String.valueOf(ready)));
        if (!ready) {
          System.out.println("client " + clientName + " need to transfer data from server");
          receiveFile();
        }
        System.out.println("client " + clientName + " " + status + " done");
        break;
      case CHECK_TABLE:
        if (putClass == null) {
          putClass = createClass();
        }
        putClass.checkTable();
        writeNoMeanMessage();
        System.out.println("client " + clientName + " " + status + " done");
        break;
      case START_TO_PUT:
        if (putClass == null) {
          putClass = createClass();
        }
        putClass.loadAndInsertData();
        while (true) {
          while (reportQueue.size() > 0) {
            sockOut.write(Bytes.toBytes(TPCHRemoteServerThread.REMOTE_PUTTING_REPORT_MSG));
            readShortMessage();
            sockOut.write(Bytes.toBytes(reportQueue.poll()));
            readShortMessage();
          }
          if (putClass.hasFinished()) { // break, ends with an write
            System.out.println("client " + clientName + " " + status + " done1");
            sockOut.write(Bytes.toBytes(TPCHRemoteServerThread.REMOTE_PUT_DONE_MSG));
            System.out.println("client " + clientName + " " + status + " done2");
            break;
          } else {
            sockOut.write(Bytes.toBytes(TPCHRemoteServerThread.REMOTE_PUTTING_MSG));
            readShortMessage();
            Thread.sleep(SLEEP_TIME * 2);
          }
        }
        break;
      case FLUSH:
        if (putClass == null) putClass = createClass();
        putClass.flush();
        writeNoMeanMessage();
        break;
      case SAY_HI:
      default:
        writeNoMeanMessage(true);
        Thread.sleep(SLEEP_TIME);
      }
    }
    System.out.println("client " + clientName + " finish all threads, return");
  }

  private TPCHRemotePutBaseClass createClass() throws IOException {
    if ("HBase".equalsIgnoreCase(testClassName)) {
      return new TPCHRemotePutHBase(confPath, additionalConf, TPCHConstants.HBASE_TABLE_NAME,
          clientDir, threadNum, statFilePath, INIT_REGION_NUM, forceFlush, reportInterval,
          reportQueue);
    } else if ("CC".equalsIgnoreCase(testClassName)) {
      return new TPCHRemotePutCC(confPath, additionalConf, TPCHConstants.CCIndex_TABLE_NAME,
          clientDir, threadNum, statFilePath, INIT_REGION_NUM, forceFlush, reportInterval,
          reportQueue);
    } else if ("CM".equalsIgnoreCase(testClassName)) {
      return new TPCHRemotePutCM(confPath, additionalConf, TPCHConstants.CMIndex_TABLE_NAME,
          clientDir, threadNum, statFilePath, INIT_REGION_NUM, forceFlush, reportInterval,
          reportQueue);
    } else if ("IR".equalsIgnoreCase(testClassName)) {
      return new TPCHRemotePutIR(confPath, additionalConf, TPCHConstants.IR_TABLE_NAME,
          clientDir, threadNum, statFilePath, INIT_REGION_NUM, forceFlush, reportInterval,
          reportQueue);
    } else if ("LCC".equalsIgnoreCase(testClassName)) {
      return new TPCHRemotePutLCC(confPath, additionalConf, TPCHConstants.LCC_TABLE_NAME,
          clientDir, threadNum, statFilePath, INIT_REGION_NUM, forceFlush, reportInterval,
          reportQueue);
    }
    throw new IOException(clientName + " meets unknown test class name: " + testClassName);
  }

  private void parseParams(String param) throws IOException {
    String parts[] = param.split("\t");
    System.out.println("client " + clientName + " recvive param: " + param);
    dataDir = parts[0];
    testClassName = parts[1];
    reportInterval = Integer.valueOf(parts[2]);
    statFilePath = dataDir + "/" + parts[3];
    forceFlush = Boolean.valueOf(parts[4]);
    clientDir = TPCHConstants.getClientDir(dataDir, clientName);
    clientName = clientName + "@" + testClassName;
  }

  private void receiveFile() throws IOException {
    for (int i = 0; i < threadNum; ++i) {
      String str = TPCHConstants.getDataFileName(clientDir, i);
      receiveFile(str);
    }
    receiveFile(statFilePath);
  }

  private void receiveFile(String fileName) throws IOException {
    System.out.println("now recv file " + fileName + " from server");
    byte[] buffer = new byte[TPCHConstants.REMOTE_FILE_TRANS_BUFFER_LEN];
    File file = new File(fileName);
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    FileOutputStream fos = new FileOutputStream(file);
    String msg = readShortMessage();
    long totalLength = Long.valueOf(msg);
    writeNoMeanMessage();
    int receivedCount = 0;
    while (true) {
      int len = sockIn.read(buffer);
      if (len > 0) {
        fos.write(buffer, 0, len);
      }
      receivedCount += len;
      if (len < 0 || receivedCount == totalLength) {
        break;
      }
    }
    fos.close();
    writeNoMeanMessage();
  }

  private boolean checkInputFileRead() {
    for (int i = 0; i < threadNum; ++i) {
      String str = TPCHConstants.getDataFileName(clientDir, i);
      if (!new File(str).exists()) {
        return false;
      }
    }
    return new File(statFilePath).exists();
  }

  private void deinit() throws IOException {
    if (sockOut != null) sockOut.close();
    if (sockIn != null) sockIn.close();
    if (sock != null) sock.close();
  }

  private void writeNoMeanMessage() throws IOException {
    writeNoMeanMessage(false);
  }

  private void writeNoMeanMessage(boolean flush) throws IOException {
    sockOut.write(TPCHRemoteServerThread.NO_MEANS_BYTES);
    if (flush) sockOut.flush();
  }

  private String readShortMessage() throws IOException {
    byte[] bufName = new byte[innerBufferLength];
    int lenInfo = 0;
    lenInfo = sockIn.read(bufName);
    return new String(bufName, 0, lenInfo);
  }

  public static void main(String[] args) throws NumberFormatException, UnknownHostException,
      IOException, InterruptedException {
    if (args.length < 5) {
      usage();
      System.out.println("current:");
      for (String str : args) {
        System.out.println(str);
      }
      return;
    }
    new TPCHRemoteClient().work(args[0], args[1], args[2], args[3], Integer.valueOf(args[4]));
  }

  public static void usage() {
    System.out
        .println("TPCHRemoteClient confPath additionalConfPath serverName thisHost threadNumber");
  }
}
