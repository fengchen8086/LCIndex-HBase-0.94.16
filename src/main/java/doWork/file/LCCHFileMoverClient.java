package doWork.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import doWork.LCCIndexConstant;

public class LCCHFileMoverClient {

  private static int shortBufferLen = 1024;
  private Socket sock = null;
  private String ip;
  private int port;
  private byte[] buffer;
  private int bufferLength;
  private InputStream sockIn;
  private OutputStream sockOut;

  public LCCHFileMoverClient(String ip, Configuration conf) throws UnknownHostException,
      IOException {
    this(ip, conf, conf.getInt(LCCIndexConstant.LCC_MOVER_BUFFER_LEN,
      LCCIndexConstant.DEFAULT_LCC_MOVER_BUFFER_LEN));
  }

  public LCCHFileMoverClient(String ip, Configuration conf, int bufferLength)
      throws UnknownHostException, IOException {
    this.ip = ip;
    this.port =
        conf.getInt(LCCIndexConstant.LCC_MOVER_PORT, LCCIndexConstant.DEFAULT_LCC_MOVER_PORT);
    this.bufferLength = bufferLength;
    buffer = new byte[this.bufferLength];
    openSocket();
  }

  private void openSocket() throws UnknownHostException, IOException {
    sock = new Socket(ip, port);
    sockIn = sock.getInputStream();
    sockOut = sock.getOutputStream();
  }

  public void deinit() throws IOException {
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

  public boolean containsFileAndCopy(String fileName) throws IOException {
    boolean ret = innerContainsFileAndCopy(fileName, 1);
    deinit();
    return ret;
  }

  public boolean deleteRemoteFile(String dirName) throws IOException {
    sockOut.write(Bytes.toBytes(LCCIndexConstant.DELETE_HEAD_MSG));
    String msg = readShortMessage();
    sockOut.write(Bytes.toBytes(dirName));
    msg = readShortMessage();
    boolean ret = false;
    if (LCCIndexConstant.DELETE_SUCCESS_MSG.equals(msg)) {
      ret = true;
    } else if (LCCIndexConstant.LCC_LOCAL_FILE_NOT_FOUND_MSG.equals(msg)) {
      ret = false;
    } else {
      System.out.println("winter msg unknow for delete file: " + dirName + ", msg: " + msg);
      ret = false;
    }
    deinit();
    return ret;
  }

  private boolean innerContainsFileAndCopy(String fileName, int times) throws IOException {
    sockOut.write(Bytes.toBytes(LCCIndexConstant.REQUIRE_HEAD_MSG));
    String msg = readShortMessage();
    sockOut.write(Bytes.toBytes(fileName));
    msg = readShortMessage();
    if (LCCIndexConstant.LCC_LOCAL_FILE_NOT_FOUND_MSG.equals(msg)) {
      // not found
      System.out.println("winter LCCHFileMoverClient local file " + fileName
          + " not found on server: " + ip);
      return false;
    } else if (LCCIndexConstant.LCC_LOCAL_FILE_FOUND_MSG.equals(msg)) {
      // found
      System.out.println("winter LCCHFileMoverClient local file " + fileName + " found on server: "
          + ip + ", the " + times + " times");
      if (new File(fileName).exists()) {
        // throw new IOException("winter LCCHFileMoverClient local file should not exists: "
        // + fileName);
        System.out
            .println("winter LCCHFileMoverClient local file exists locally, why call remote? "
                + new Exception());
        new Exception().printStackTrace();
        return true;
      }
      try {
        return transferFile(fileName);
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
        throw new IOException("NoSuchAlgorithmException in transferFile, file: " + fileName, e);
      } catch (IOException e) {
        e.printStackTrace();
        throw new IOException("IOException in transferFile, file: " + fileName, e);
      }
    } else {
      if (msg == null) System.out.println("winter LCCHFileMoverClient msg is null");
      else System.out.println("msg LCCHFileMoverClient not understood: " + msg);
      return false;
    }
  }

  private boolean transferFile(String fileName) throws IOException, NoSuchAlgorithmException {
    File file = new File(fileName);
    mkParentDirs(file);
    file.createNewFile();
    FileOutputStream fos = new FileOutputStream(file);
    sockOut.write(fileName.getBytes()); // write again to tell the server ready to recv data
    // write into local file!
    String msg = readShortMessage();
    System.out.println("LCCHFileMoverClient file length: " + msg + " of file: " + fileName);
    long totalLength = Long.valueOf(msg);
    sockOut.write(msg.getBytes()); // write again to tell the server ready to recv data
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
    return true;
  }

  public static void mkParentDirs(File file) {
    Stack<File> fileStack = new Stack<File>();
    file = file.getParentFile();
    while (!file.exists()) {
      fileStack.push(file);
      file = file.getParentFile();
    }
    while (!fileStack.empty()) {
      fileStack.pop().mkdir();
    }
  }
}