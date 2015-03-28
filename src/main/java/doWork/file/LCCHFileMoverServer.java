package doWork.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import doWork.LCCIndexConstant;

public class LCCHFileMoverServer implements Runnable {

  private int serverPort;
  private int bufferLength = 0;
  private AtomicInteger runningThreads = new AtomicInteger(0);

  public LCCHFileMoverServer(Configuration conf) throws IOException {
    serverPort =
        conf.getInt(LCCIndexConstant.LCC_MOVER_PORT, LCCIndexConstant.DEFAULT_LCC_MOVER_PORT);
    bufferLength =
        conf.getInt(LCCIndexConstant.LCC_MOVER_BUFFER_LEN,
          LCCIndexConstant.DEFAULT_LCC_MOVER_BUFFER_LEN);
  }

  private boolean keepAlive = true;

  @Override
  public void run() {
    ServerSocket ss = null;
    try {
      ss = new ServerSocket(serverPort);
      System.out.println("winter LCCHFileMoverServer build on port: " + serverPort);
      while (keepAlive) {
        // should change to nio, otherwise will block here!
        // may be connecting localhost is possible
        Socket s = ss.accept();
        System.out.println("winter LCCHFileMoverServer running thread number: "
            + runningThreads.incrementAndGet());
        new ServerThread(s, bufferLength).start();
      }
      while (runningThreads.intValue() > 0) {
        System.out.println("winter sleeping for " + runningThreads.intValue() + " threads");
        Thread.sleep(5000);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    System.out.println("winter LCCHFileMoverServer deinit done");
  }

  public void setToClose() {
    synchronized (this) {
      keepAlive = false;
    }
  }

  class ServerThread extends Thread {
    private int shortBufferLen = 1024;
    private Socket sock = null;
    private OutputStream sockOut;
    InputStream sockIn;
    byte[] buffer;

    public ServerThread(Socket sock, int bufferLen) throws IOException {
      this.sock = sock;
      sockOut = sock.getOutputStream();
      sockIn = sock.getInputStream();
      buffer = new byte[bufferLen];
    }

    public void run() {
      try {
        work();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      } finally {
        try {
          runningThreads.decrementAndGet();
          deinit();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    private void deinit() throws IOException {
      if (sockOut != null) sockOut.close();
      if (sockIn != null) sockIn.close();
      if (sock != null) sock.close();
      buffer = null;
    }

    private String readShortMessage() throws IOException {
      return readShortMessage(null);
    }

    private String readShortMessage(String prevMessage) throws IOException {
      byte[] bufName = new byte[shortBufferLen];
      int lenInfo = 0;
      lenInfo = sockIn.read(bufName);
      if (lenInfo == -1 && prevMessage != null) {
        
        if (prevMessage.startsWith(LCCIndexConstant.DELETE_HEAD_MSG)) {
          return prevMessage.substring(LCCIndexConstant.DELETE_HEAD_MSG.length());
        } else if (prevMessage.startsWith(LCCIndexConstant.REQUIRE_HEAD_MSG)) {
          return prevMessage.substring(LCCIndexConstant.REQUIRE_HEAD_MSG.length());
        }
      }
      return new String(bufName, 0, lenInfo);
    }

    private void deleteFile(String targetName) throws IOException {
      File file = new File(targetName);
      if (file.exists()) {
        if (file.isDirectory()) {
          FileUtils.deleteDirectory(file);
        } else {
          file.delete();
        }
        sockOut.write(Bytes.toBytes(LCCIndexConstant.DELETE_SUCCESS_MSG));
      } else {
        // actually, any thing is ok
        sockOut.write(Bytes.toBytes(LCCIndexConstant.LCC_LOCAL_FILE_NOT_FOUND_MSG));
      }
    }

    private void transferFile(String targetName) throws IOException {
      File file = new File(targetName);
      // server check local, and say no if not exists
      if (!file.exists()) {
        sockOut.write(LCCIndexConstant.LCC_LOCAL_FILE_NOT_FOUND_MSG.getBytes());
        System.out.println("winter LCCHFileMoverServer can not fild file: " + targetName);
        return;
      }
      // File exists!
      if (!file.isFile()) {
        sockOut.write(LCCIndexConstant.LCC_LOCAL_FILE_NOT_FOUND_MSG.getBytes());
        throw new IOException(
            "winter LCCHFileMoverServer found file but not a file (may be a dir?) : " + targetName);
      }
      sockOut.write(LCCIndexConstant.LCC_LOCAL_FILE_FOUND_MSG.getBytes());
      sockOut.flush();
      FileInputStream fis = new FileInputStream(file); // read local file
      // send file to client
      int len = 0;
      readShortMessage(); // read message just wait for ack from client
      System.out.println("winter LCCHFileMoverServer write file length: "
          + String.valueOf(file.length()));
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
      // delete local file!
      file.delete();
    }

    private void work() throws IOException, NoSuchAlgorithmException {
      String msg = readShortMessage();
      sockOut.write(Bytes.toBytes(LCCIndexConstant.NO_MEANING_MSG));
      String targetName = readShortMessage(msg);
      if (LCCIndexConstant.DELETE_HEAD_MSG.equals(msg)) {
        deleteFile(targetName);
      } else if (LCCIndexConstant.REQUIRE_HEAD_MSG.equals(msg)) {
        transferFile(targetName);
      }
    }
  }
}
