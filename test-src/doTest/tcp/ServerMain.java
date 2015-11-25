package doTest.tcp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import doWork.LCCIndexConstant;
import doWork.file.LCCHFileMoverClient;

public class ServerMain implements Runnable {

  private int serverPort;
  int bufferLength = 0;

  public ServerMain(Configuration conf) throws IOException {
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
        new ServerThread(s, bufferLength).start();
      }
    } catch (IOException e) {
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
      byte[] bufName = new byte[shortBufferLen];
      int lenInfo = 0;
      lenInfo = sockIn.read(bufName);
      return new String(bufName, 0, lenInfo);
    }

    private void work() throws IOException, NoSuchAlgorithmException {
      String targetName = readShortMessage();
      File file = new File(targetName);
      System.out.println("winter want to find on this server server: " + targetName);
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
      System.out.println("winter LCCHFileMoverServer found file: " + targetName);
      sockOut.write(LCCIndexConstant.LCC_LOCAL_FILE_FOUND_MSG.getBytes());
      sockOut.flush();
      FileInputStream fis = new FileInputStream(file); // read local file
      // send file to client
      int len = 0;
      readShortMessage(); // read message just wait for ack from client
      sockOut.write(String.valueOf(file.length()).getBytes());
      while (true) {
        len = fis.read(buffer);
        if (len > 0) {
          sockOut.write(buffer, 0, len); // write data!
        } else {
          break;
        }
      }
      fis.close();
      String newFileName = "/home/fengchen/debug/" + targetName;
      File newFile = new File(newFileName);
      if (!newFile.exists()) {
        LCCHFileMoverClient.mkParentDirs(newFile);
        FileUtils.copyFile(file, newFile);
      }
      compareMD5(targetName);
      sock.shutdownOutput();
      // delete local file!
      System.out.println("winter delete file after read from local: " + targetName);
      file.delete();
    }

    private void compareMD5(String targetName) throws IOException, NoSuchAlgorithmException {
      System.out.println("winter LCCHFileMoverServer compare MD5: " + targetName);
      MessageDigest md = null;
      byte[] digest = null;
//      digest = LCCHFileMoverClient.calMD5(targetName);
      byte[] tempBuf = new byte[1024];
      int lenInfo = sockIn.read(tempBuf);
      if (lenInfo != digest.length) {
        throw new IOException("winter MD5 difference of file: " + targetName + ", remote length: "
            + lenInfo + " vs local length: " + digest.length);
      }
      for (int i = 0; i < digest.length; ++i) {
        if (tempBuf[i] != digest[i]) {
          sockOut.write("blabla".getBytes());
          sock.shutdownOutput();
          throw new IOException("winter MD5 difference of file: " + targetName + ", at index " + i
              + ", " + (short) tempBuf[i] + " vs " + (short) digest[i]);
        }
      }
      sockOut.write(LCCIndexConstant.TCP_BYE_MSG.getBytes());
    }
  }
}
