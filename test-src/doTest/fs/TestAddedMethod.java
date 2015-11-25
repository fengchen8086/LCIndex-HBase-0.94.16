package doTest.fs;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.WinterIP;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class TestAddedMethod {

  public static String HADOOP_CONF_PATH = "/home/winter/softwares/hadoop-1.0.4/conf";
  static String localStr = "/home/winter/temp/about-LCIndex.md";
  static String hdfsStr = "/winter/test";

  public static void main(String[] args) throws Exception {
    String hadoop_conf_path = null;
    String local = localStr;
    String hdfs = hdfsStr;
    short replication = 2;
    if (args.length >= 1) {
      hadoop_conf_path = args[0];
    }
    if (args.length >= 2) local = args[1];
    if (args.length >= 3) hdfs = args[2];
    if (args.length >= 4) replication = Short.valueOf(args[3]);
    if (hadoop_conf_path == null) hadoop_conf_path = HADOOP_CONF_PATH;
    System.out.println("use hadoop_conf_path: " + hadoop_conf_path);
    System.out.println(InetAddress.getLocalHost().getHostAddress());
    Configuration conf = new Configuration();
    if (!InetAddress.getLocalHost().getHostAddress().equals("127.0.0.1")) {
      conf.set("fs.default.name", "hdfs://lingcloud25:9000");
    }
    conf.addResource(new Path(HADOOP_CONF_PATH + "/core-site.xml"));
    conf.addResource(new Path(HADOOP_CONF_PATH + "/hdfs-site.xml"));

    new TestAddedMethod().testLocalCopy(conf, hadoop_conf_path, local, hdfs, replication);
  }

  public void testLocalCopy(Configuration conf, String hadoop_conf_path, String localStr,
      String hdfsStr, short numOfReplica) throws IOException, InterruptedException {
    DistributedFileSystem hdfs = (DistributedFileSystem) DistributedFileSystem.get(conf);
    InetAddress localIP = InetAddress.getLocalHost();
    WinterIP ip = new WinterIP(localIP.getHostAddress(), localIP.getHostName());
    if (hdfs.exists(new Path(hdfsStr))) {
      hdfs.delete(new Path(hdfsStr), true);
    }
    System.out.println(ip.getHostName());
    System.out.println(ip.getIP());
    hdfs.mWinterCopyFromLocalFile(new Path(localStr), new Path(hdfsStr), numOfReplica, ip);
    // hdfs.copyFromLocalFile(new Path(localStr), new Path(hdfsStr));
  }
}
