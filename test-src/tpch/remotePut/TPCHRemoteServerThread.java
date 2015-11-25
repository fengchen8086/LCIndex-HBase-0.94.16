package tpch.remotePut;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import tpch.put.TPCHConstants;

public class TPCHRemoteServerThread extends Thread {

  enum RemotePutStatus {
    SEND_PARAM, CHECK_DATA, CHECK_TABLE, START_TO_PUT, FLUSH, CLOSE, SAY_HI
  }

  private final int innerBufferLength = 2048;
  private final int SLEEP_TIME = 100;
  public static final String REMOTE_PUTTING_MSG = "REMOTE_IS_PUTTING_MSG";
  public static final String REMOTE_PUTTING_REPORT_MSG = "REMOTE_PUTTING_REPORT_MSG";
  public static final String REMOTE_PUT_DONE_MSG = "REMOTE_PUT_DONE_MSG";
  public static final byte[] NO_MEANS_BYTES = Bytes.toBytes("hi,ICPP");
  private static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

  private final Socket sock;
  private final OutputStream sockOut;
  private final InputStream sockIn;

  private final String clientName;
  private final List<String> unFinishedClients;
  private final String dataDir;
  private final String param;
  private RemotePutStatus status;
  private int threadNum;
  private String statFilePath;
  private Object syncObj;

  public TPCHRemoteServerThread(Socket sock, List<String> unFinishedClients, String dataDir,
      String param, int threadNum, String statFileName, Object syncObj) throws IOException {
    this.sock = sock;
    status = RemotePutStatus.SAY_HI;
    sockOut = sock.getOutputStream();
    sockIn = sock.getInputStream();
    clientName = readShortMessage();
    this.syncObj = syncObj;
    this.threadNum = threadNum;
    System.out.println("the remote process is: " + clientName + ", want " + threadNum + " threads");
    this.dataDir = dataDir;
    this.param = param;
    this.statFilePath = dataDir + "/" + statFileName;
    this.unFinishedClients = unFinishedClients;
  }

  private void deinit() throws IOException {
    if (sockOut != null) sockOut.close();
    if (sockIn != null) sockIn.close();
    if (sock != null) sock.close();
  }

  private void setCurrentFinished() {
    long start = System.currentTimeMillis();
    synchronized (syncObj) {
      if (unFinishedClients.contains(clientName)) {
        unFinishedClients.remove(clientName);
      }
    }
    System.out.println("server thread spend " + (System.currentTimeMillis() - start) / 1000.0
        + " seconds to mark " + clientName + " free for status " + status);
    this.status = RemotePutStatus.SAY_HI;
  }

  private String readShortMessage() throws IOException {
    byte[] bufName = new byte[innerBufferLength];
    int lenInfo = 0;
    lenInfo = sockIn.read(bufName);
    return new String(bufName, 0, lenInfo);
  }

  @Override
  public void run() {
    try {
      // 1sr. read hostname
      while (status != RemotePutStatus.CLOSE) {
        switch (status) {
        case SEND_PARAM:
          sockOut.write(Bytes.toBytes(String.valueOf(RemotePutStatus.SEND_PARAM)));
          readShortMessage();
          sockOut.write(Bytes.toBytes(param));
          readShortMessage();
          setCurrentFinished();
          break;
        case CHECK_DATA:
          sockOut.write(Bytes.toBytes(String.valueOf(RemotePutStatus.CHECK_DATA)));
          boolean ready = Boolean.valueOf(readShortMessage());
          if (!ready) {
            System.out.println("remote client data not ready, transfer file now: " + clientName);
            transferFile();
          }
          setCurrentFinished();
          break;
        case CHECK_TABLE:
          sockOut.write(Bytes.toBytes(String.valueOf(RemotePutStatus.CHECK_TABLE)));
          readShortMessage();
          setCurrentFinished();
        case START_TO_PUT:
          sockOut.write(Bytes.toBytes(String.valueOf(RemotePutStatus.START_TO_PUT)));
          while (true) {
            String msg = readShortMessage();
            if (REMOTE_PUT_DONE_MSG.equals(msg)) { // ends with an read
              setCurrentFinished();
              break;
            } else if (REMOTE_PUTTING_MSG.equals(msg)) {
              writeNoMeanMessage();
              Thread.sleep(SLEEP_TIME * 1);
            } else if (REMOTE_PUTTING_REPORT_MSG.equals(msg)) {
              writeNoMeanMessage();
              msg = readShortMessage();
              writeNoMeanMessage();
              System.out.println("coffey from " + clientName + ": " + msg + " at time "
                  + dateFormat.format(new Date()));
            } else {
              throw new IOException("coffey meet unknown message in putting: " + msg);
            }
          }
          break;
        case FLUSH:
          sockOut.write(Bytes.toBytes(String.valueOf(RemotePutStatus.FLUSH)));
          readShortMessage();
          setCurrentFinished();
          break;
        case SAY_HI:
        default:
          sockOut.write(Bytes.toBytes(String.valueOf(RemotePutStatus.SAY_HI)));
          readShortMessage();
          Thread.sleep(SLEEP_TIME);
        }
      }
      // now close
      sockOut.write(Bytes.toBytes(String.valueOf(RemotePutStatus.CLOSE)));
      String msg = readShortMessage();
      System.out.println("coffey report " + clientName + ": " + msg);
      writeNoMeanMessage();
      int times = Integer.valueOf(readShortMessage());
      writeNoMeanMessage();
      while (times > 0) {
        System.out.println("coffey report " + clientName + ": " + readShortMessage());
        writeNoMeanMessage();
        --times;
      }
      readShortMessage();
      System.out.println("client " + clientName + " close");
      setCurrentFinished();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      try {
        deinit();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void transferFile() throws IOException {
    String clientDir = TPCHConstants.getClientDir(dataDir, clientName);
    for (int i = 0; i < threadNum; ++i) {
      String wantedFile = TPCHConstants.getDataFileName(clientDir, i);
      sendOneFile(wantedFile);
    }
    sendOneFile(statFilePath);
  }

  private void sendOneFile(String fileName) throws IOException {
    byte[] buffer = new byte[TPCHConstants.REMOTE_FILE_TRANS_BUFFER_LEN];
    File file = new File(fileName);
    FileInputStream fis = new FileInputStream(file); // read local file
    int len = 0;
    sockOut.write(Bytes.toBytes(String.valueOf(file.length())));
    readShortMessage();
    while (true) {
      len = fis.read(buffer);
      if (len > 0) {
        sockOut.write(buffer, 0, len); // write data!
      } else {
        break;
      }
    }
    fis.close();
    readShortMessage();
  }

  private void writeNoMeanMessage() throws IOException {
    writeNoMeanMessage(false);
  }

  private void writeNoMeanMessage(boolean flush) throws IOException {
    sockOut.write(TPCHRemoteServerThread.NO_MEANS_BYTES);
    if (flush) sockOut.flush();
  }

  public RemotePutStatus getStatus() {
    return status;
  }

  public void setStatus(RemotePutStatus status) {
    this.status = status;
  }

  public String getClientName() {
    return clientName;
  }
}
