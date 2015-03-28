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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;

import doWork.LCCIndexConstant;

public class LCCHFileMoverClient {

  public enum RemoteStatus {
    NOT_EXIST, IN_QUEUE_WAITING, SUCCESS, UNKNOWN;
  }

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

  public RemoteStatus copyRemoteFile(String fileName, Path hdfsPath, boolean processNow)
      throws IOException {
    RemoteStatus ret = innerContainsFileAndCopy(fileName, hdfsPath, processNow);
    deinit();
    return ret;
  }

  public RemoteStatus deleteRemoteFile(String dirName) throws IOException {
    sockOut.write(Bytes.toBytes(LCCIndexConstant.DELETE_HEAD_MSG));
    String msg = readShortMessage();
    sockOut.write(Bytes.toBytes(dirName));
    msg = readShortMessage();
    RemoteStatus ret = RemoteStatus.UNKNOWN;
    if (LCCIndexConstant.DELETE_SUCCESS_MSG.equals(msg)) {
      ret = RemoteStatus.SUCCESS;
    } else if (LCCIndexConstant.LCC_LOCAL_FILE_NOT_FOUND_MSG.equals(msg)) {
      ret = RemoteStatus.NOT_EXIST;
    } else {
      System.out.println("winter msg unknow for delete file: " + dirName + ", msg: " + msg);
    }
    deinit();
    return ret;
  }

  private RemoteStatus innerContainsFileAndCopy(String realName, Path hdfsPath, boolean processNow)
      throws IOException {
    sockOut.write(Bytes.toBytes(LCCIndexConstant.REQUIRE_HEAD_MSG));
    String msg = readShortMessage();
    sockOut.write(Bytes.toBytes(realName));
    msg = readShortMessage();
    if (LCCIndexConstant.LCC_LOCAL_FILE_NOT_FOUND_MSG.equals(msg)) {
      sockOut.write(Bytes.toBytes(hdfsPath.toString()));
      msg = readShortMessage();
      if (LCCIndexConstant.LCC_LOCAL_FILE_NOT_FOUND_MSG.equals(msg)) {
        return RemoteStatus.NOT_EXIST;
      } else {
        System.out.println("winter LCCHFileMoverClient file " + realName
            + " found on server queues: " + msg + ", ip: " + ip);
        sockOut.write(Bytes.toBytes(String.valueOf(processNow)));
        if (processNow) {
          try {
            return transferFile(realName);
          } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new IOException("NoSuchAlgorithmException in transferFile, file: " + realName, e);
          } catch (IOException e) {
            e.printStackTrace();
            throw new IOException("IOException in transferFile, file: " + realName, e);
          }
        } else {
          return RemoteStatus.IN_QUEUE_WAITING;
        }
      }
    } else if (LCCIndexConstant.LCC_LOCAL_FILE_FOUND_MSG.equals(msg)) {
      // found
      System.out.println("winter LCCHFileMoverClient file " + realName + " found on server: " + ip);
      try {
        sockOut.write(Bytes.toBytes(LCCIndexConstant.NO_MEANING_MSG));
        return transferFile(realName);
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
        throw new IOException("NoSuchAlgorithmException in transferFile, file: " + realName, e);
      } catch (IOException e) {
        e.printStackTrace();
        throw new IOException("IOException in transferFile, file: " + realName, e);
      }
    } else {
      System.out.println("msg LCCHFileMoverClient not understood: " + msg);
      return RemoteStatus.UNKNOWN;
    }
  }

  private RemoteStatus transferFile(String fileName) throws IOException, NoSuchAlgorithmException {
    File file = new File(fileName);
    mkParentDirs(file);
    file.createNewFile();
    FileOutputStream fos = new FileOutputStream(file);
    // write into local file!
    String msg = readShortMessage();
    // System.out.println("LCCHFileMoverClient file length: " + msg + " of file: " + fileName);
    long totalLength = Long.valueOf(msg);
    // write again to tell the server, client is ready to recv data
    sockOut.write(Bytes.toBytes(LCCIndexConstant.NO_MEANING_MSG));
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
    return RemoteStatus.SUCCESS;
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