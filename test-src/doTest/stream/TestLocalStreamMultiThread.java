package doTest.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class TestLocalStreamMultiThread {

  static String defaultHadoopConfPath = "/home/winter/softwares/hadoop-1.0.4";
  static String defaultHBaseConfPath = "/home/winter/softwares/hbase-0.94.16";
  // static String defaultFileName = "/home/winter/temp/test.dat";
  static String defaultFileName = "/home/winter/c00fb4e210104032b07ec6206d334af2";

  private int turn = 0;
  private int THREAD_NUM = 10;
  private int SLEEP_TIME = 2 * 1000;
  private Random random = new Random();

  private synchronized int getTurn() {
    return turn;
  }

  private synchronized void addTurn() {
    turn = random.nextInt(THREAD_NUM);
  }

  public static void main(String args[]) throws IOException, InterruptedException {
    // String hadoopConfPath = "/home/winter/softwares/hbase-0.94.16";
    String hadoopConfPath = null;
    String fileName = null;
    String hbaseConfPath = null;
    if (args.length >= 3) {
      hadoopConfPath = args[0];
      hbaseConfPath = args[1];
      fileName = args[2];
    }
    hadoopConfPath = hadoopConfPath == null ? defaultHadoopConfPath : hadoopConfPath;
    hbaseConfPath = hbaseConfPath == null ? defaultHBaseConfPath : hbaseConfPath;
    fileName = fileName == null ? defaultFileName : fileName;
    new TestLocalStreamMultiThread().work(hadoopConfPath, hbaseConfPath, fileName);
  }

  private void work(String hadoopConfPath, String hbaseConfPath, String fileName)
      throws IOException, InterruptedException {
    Configuration hadoopConf = new Configuration();
    System.out.println("use hadoop_conf_path: " + hadoopConfPath);
    hadoopConf.addResource(new Path(hadoopConfPath + "/core-site.xml"));
    hadoopConf.addResource(new Path(hadoopConfPath + "/hdfs-site.xml"));
    // hadoopConf.addResource(new Path(hadoopConfPath + "/hbase-site.xml"));

    System.out.println("use hadoop_conf_path: " + hbaseConfPath);
    Configuration hbaseConf = HBaseConfiguration.create();
    hbaseConf.addResource(hbaseConfPath + "/hbase-site.xml");

    readStream(hadoopConf, hbaseConf, fileName);
  }

  private void readStream(Configuration hadoopConf, Configuration hbaseConf, String fileName)
      throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(hbaseConf);
    System.out.println("file system is: " + fs);

    Path p = new Path(fileName);
    FileStatus status = fs.getFileStatus(p);
    long fileLength = status.getLen();
    System.out.println("length: " + fileLength);
    if (fileLength > Integer.MAX_VALUE) {
      System.out.println("file length > INT.MAX " + Integer.MAX_VALUE);
      return;
    }
    ArrayList<Byte> checkList = readFileList(fs, fileName);
    FSDataInputStream fsdis = fs.open(new Path(fileName));
    Thread[] thread = new Thread[THREAD_NUM];
    for (int i = 0; i < THREAD_NUM; ++i) {
      thread[i] = new Thread(new ThreadReader(i, fsdis, (int) fileLength, checkList));
      thread[i].start();
    }
    for (int i = 0; i < THREAD_NUM; ++i) {
      thread[i].join();
    }
  }

  private ArrayList<Byte> readFileList(FileSystem fs, String fileName) throws IOException {
    int readLength = 102400;
    byte[] buffer = new byte[readLength];
    ArrayList<Byte> list = new ArrayList<Byte>();
    FSDataInputStream fsdis = fs.open(new Path(fileName));
    int len = 0;
    while (len >= 0) {
      len = fsdis.read(buffer, 0, readLength);
      for (int i = 0; i < len; ++i) {
        list.add(buffer[i]);
      }
    }
    fsdis.close();
    return list;
  }

  class ThreadReader implements Runnable {

    private int id;
    private FSDataInputStream istream;
    private final int MAX_READ_LEN = 63865;
    private Random random = new Random();
    private int fileLength;
    private byte[] buffer = new byte[MAX_READ_LEN];
    private ArrayList<Byte> checkList;
    private int counter = 0;

    public ThreadReader(int id, FSDataInputStream istream, int fileLength) {
      this(id, istream, fileLength, TestCreateLocalStream.getFinalList());
    }

    public ThreadReader(int id, FSDataInputStream istream, int fileLength, ArrayList<Byte> checkList) {
      this.id = id;
      this.istream = istream;
      this.fileLength = fileLength;
      this.checkList = checkList;
    }

    @Override
    public void run() {
      while (true) {
        int turn = getTurn();
        try {
          if (turn == id) {
            System.out.println("winter thread " + id + " running");
            processRead();
            addTurn();
            System.out.println("winter success process without error round: " + (++counter));
          } else {
            Thread.sleep(SLEEP_TIME);
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
          return;
        }
      }
    }

    private void processRead() throws IOException {
      int readLen = random.nextInt(MAX_READ_LEN) + 1;
      int seekTo = random.nextInt(fileLength - readLen - 1);
      // so will not reach the EOF
      System.out.println("winter thread " + id + " seek to " + seekTo + " for length: " + readLen
          + ", thread: " + Thread.currentThread().getId());
      istream.seek(seekTo);
      int ret = istream.read(buffer, 0, readLen);
      if (ret > 0) {
        for (int i = 0; i < ret; ++i) {
          if (buffer[i] != checkList.get(i + seekTo)) {
            mWinterPrintBuf(buffer, 0, Math.min(ret, i + 5), "winter found difference");
            System.out.println("winter thread " + id + " at file offset: " + (seekTo + i)
                + ", read offset: " + i);
            System.out.println("read byte: " + buffer[i]);
            System.out.println("check byte: " + checkList.get(i + seekTo));
            throw new IOException("difference!");
          }
        }
      }
    }
  }

  public static void mWinterPrintBuf(byte[] buf, int offset, int length, String prefix) {
    mWinterPrintBuf(buf, offset, length, prefix, true, true);
  }

  public static void mWinterPrintBuf(byte[] buf, int offset, int length, String prefix,
      boolean showChar, boolean showInt) {
    if (!showChar && !showInt) return;
    String s1 = "", s2 = "";
    if (offset < 0) {
      System.out.println("winter print set to zero because offset=" + offset);
      offset = 0;
    }
    for (int i = 0; i < length && i < buf.length; ++i) {
      s1 = s1 + (char) buf[i + offset];
      s2 = s2 + "/" + (short) buf[i + offset];
    }
    if (showChar) System.out.println(prefix + ", s1 in char: " + s1);
    if (showInt) System.out.println(prefix + ", s2 in short: " + s2);
  }
}