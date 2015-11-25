package doTest.tcp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Stack;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.io.Files;

import doWork.LCCIndexConstant;

public class ClientMain {

  private static int shortBufferLen = 1024;
  private Socket sock = null;
  private String ip;
  private int port;
  private byte[] buffer;
  private int bufferLength;
  private InputStream sockIn;
  private OutputStream sockOut;
  private final int MAX_TRY_TIMES = 3;

  public ClientMain(String ip, Configuration conf) throws UnknownHostException, IOException {
    this.ip = ip;
    this.port =
        conf.getInt(LCCIndexConstant.LCC_MOVER_PORT, LCCIndexConstant.DEFAULT_LCC_MOVER_PORT);
    bufferLength =
        conf.getInt(LCCIndexConstant.LCC_MOVER_BUFFER_LEN,
          LCCIndexConstant.DEFAULT_LCC_MOVER_BUFFER_LEN);
    buffer = new byte[bufferLength];
    openSocket();
  }

  private void openSocket() throws UnknownHostException, IOException {
    sock = new Socket(ip, port);
    sockIn = sock.getInputStream();
    sockOut = sock.getOutputStream();
  }

  private void closeSocket() throws IOException {
    if (sockOut != null) sockOut.close();
    if (sockIn != null) sockIn.close();
    if (sock != null) sock.close();
  }

  private void resetSocket() throws UnknownHostException, IOException {
    closeSocket();
    openSocket();
  }

  private String readShortMessage() throws IOException {
    byte[] bufName = new byte[shortBufferLen];
    int lenInfo = 0;
    lenInfo = sockIn.read(bufName);
    return new String(bufName, 0, lenInfo);
  }

  // the problem is socket reset, so we must reset the socket!
  public boolean containsFileAndCopy(String fileName) throws IOException {
    try {
      return innerContainsFileAndCopy(fileName, 1);
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      throw new IOException("NoSuchAlgorithmException in LCCHFileMoverClient", e);
    }

    // int counter = 1;
    // try {
    // while (true) {
    // try {
    // return innerContainsFileAndCopy(fileName, counter);
    // } catch (IOException e) {
    // File file = new File(fileName);
    // if (file.exists()) {
    // System.out.println("winter fail transFileWithTries the " + counter + "th times");
    // file.delete();
    // }
    // resetSocket();
    // if (counter >= MAX_TRY_TIMES) {
    // throw new IOException("winter still fail after tried " + MAX_TRY_TIMES + " times", e);
    // }
    // } catch (NoSuchAlgorithmException e) {
    // throw new IOException("NoSuchAlgorithmException in LCCHFileMoverClient", e);
    // }
    // counter++;
    // }
    // } finally {
    // closeSocket();
    // }
  }

  private boolean innerContainsFileAndCopy(String fileName, int times)
      throws NoSuchAlgorithmException, IOException {
    sockOut.write(fileName.getBytes());
    String msg = readShortMessage();
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
        throw new IOException("winter LCCHFileMoverClient local file should not exists: "
            + fileName);
      }
      return transferFile(fileName);
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
    int totalLength = Integer.valueOf(readShortMessage());
    int receivedCount = 0;
    while (true) {
      int len = sockIn.read(buffer);
      if (len > 0) {
        fos.write(buffer, 0, len);
      }
      receivedCount += len;
      System.out.println("this time length: " + len + ", total " + receivedCount + "/"
          + totalLength);
      if (len < 0 || receivedCount == totalLength) {
        break;
      }
    }
    fos.close();
    String newFileName = "/home/fengchen/debug/" + fileName;
    File newFile = new File(newFileName);
    if (!newFile.exists()) {
      mkParentDirs(newFile);
      FileUtils.copyFile(file, newFile);
    }
    compareMD5(fileName);
    System.out.println("winter LCCHFileMoverClient done with MD5 check moving file: " + fileName);
    return true;
  }

  public static byte[] calMD5(String fileName) throws NoSuchAlgorithmException, IOException {
    MessageDigest md = null;
//    InputStream is = Files.newInputStream(Paths.get(fileName));
    md = MessageDigest.getInstance("MD5");
//    DigestInputStream dis = new DigestInputStream(is, md);
    return md.digest();
  }

  private boolean compareMD5(String fileName) throws NoSuchAlgorithmException, IOException {
    System.out.println("winter LCCHFileMoverClient compare MD5: " + fileName);
    byte[] digest = calMD5(fileName);
    sockOut.write(digest);
    String str = readShortMessage();
    if (!LCCIndexConstant.TCP_BYE_MSG.equals(str)) {
      throw new IOException("winter MD5 error in transfering file: " + fileName);
    }
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