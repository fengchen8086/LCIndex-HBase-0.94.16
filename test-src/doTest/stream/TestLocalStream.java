package doTest.stream;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class TestLocalStream {

  static String defaultHadoopConfPath = "/home/winter/softwares/hadoop-1.0.4";
  static String defaultHBaseConfPath = "/home/winter/softwares/hbase-0.94.16";
  static String defaultFileName = "/home/winter/docs/btrace-datampi.bak";
  static final int BUF_LENGTH = 1024;

  public static void main(String args[]) throws IOException {
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
    new TestLocalStream().work(hadoopConfPath, hbaseConfPath, fileName);
  }

  private void work(String hadoopConfPath, String hbaseConfPath, String fileName)
      throws IOException {
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
      throws IOException {
    FileSystem fs = FileSystem.get(hbaseConf);
    System.out.println("file system get from conf: " + fs.toString());

    Path p = new Path(fileName);
    FileStatus status = fs.getFileStatus(p);
    long fileLength = status.getLen();
    System.out.println("length: " + fileLength);
    FSDataInputStream fsdis = fs.open(new Path(fileName));
    byte[] buffer = new byte[BUF_LENGTH];
    int len = 0;
    StringBuilder sb = new StringBuilder();
    boolean hasSeekBack = false;

    fsdis.seek(fileLength / 2);
    System.out.println("pos: " + fsdis.getPos());
    while (len >= 0) {
      len = fsdis.read(buffer, 0, BUF_LENGTH);
      System.out.println("new read len: " + len);
      for (int i = 0; i < len; ++i) {
        sb.append((char) buffer[i]);
      }
      if (!hasSeekBack) {
        hasSeekBack = true;
        fsdis.seek(0);
      }
    }
    System.out.println(sb);
    System.out.println("now this is the second time!");
    sb = new StringBuilder();
    while (len >= 0) {
      len = fsdis.read(buffer, 0, BUF_LENGTH);
      System.out.println("new read len: " + len);
      for (int i = 0; i < len; ++i) {
        sb.append((char) buffer[i]);
      }
    }
    System.out.println(sb);
  }
}