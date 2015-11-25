package doTest.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class HDFS {

  public static String HADOOP_CONF_PATH = "/home/winter/softwares/hadoop-1.0.4/conf";

  public static void main(String[] args) throws Exception {
    String hadoop_conf_path = null;
    if (args.length == 1) {
      hadoop_conf_path = args[0];
    }
    // getHDFSNodes();
    // getFileLocal();
    getHDFSNodes(hadoop_conf_path);
    // printDataNodeInfo(hadoop_conf_path);
  }

  public static void printDataNodeInfo(String hadoop_conf_path) throws IOException,
      InterruptedException {
    Configuration conf = new Configuration();
    if (hadoop_conf_path == null) {
      hadoop_conf_path = HADOOP_CONF_PATH;
    } else {
      conf.set("fs.default.name", "hdfs://lingcloud25:9000");
    }
    System.out.println("use hadoop_conf_path: " + hadoop_conf_path);
    conf.addResource(new Path(HADOOP_CONF_PATH + "/core-site.xml"));
    conf.addResource(new Path(HADOOP_CONF_PATH + "/hdfs-site.xml"));

    FileSystem fs = FileSystem.get(conf);
    System.out.println("file system get from conf: " + fs.toString());
    DistributedFileSystem hdfs = (DistributedFileSystem) DistributedFileSystem.get(conf);

    while (true) {
      DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
      for (DatanodeInfo di : dataNodeStats) {
        System.out.println("winter datanode: " + di.getHostName() + " has Xceiver "
            + di.getXceiverCount());
        Thread.sleep(1000);
      }
    }
  }

  public static void getFileLocal() throws Exception {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/winter/softwares/hadoop-1.0.4/conf/core-site.xml"));
    conf.addResource(new Path("/home/winter/softwares/hadoop-1.0.4/conf/hdfs-site.xml"));

    FileSystem hdfs = FileSystem.get(conf);
    Path fpath = new Path("/fill-data/d1/.test");

    FileStatus fileStatus = hdfs.getFileStatus(fpath);
    BlockLocation[] blkLocations = hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

    int blockLen = blkLocations.length;

    for (int i = 0; i < blockLen; ++i) {
      String[] hosts = blkLocations[i].getHosts();
      System.out.println("block_" + i + "_location:" + hosts[i]);
    }
  }

  public static void getHDFSNodes(String hadoop_conf_path) throws Exception {
    Configuration conf = new Configuration();
    if (hadoop_conf_path == null) {
      hadoop_conf_path = HADOOP_CONF_PATH;
    } else {
      conf.set("fs.default.name", "hdfs://lingcloud25:9000");
    }
    System.out.println("use hadoop_conf_path: " + hadoop_conf_path);
    conf.addResource(new Path(HADOOP_CONF_PATH + "/core-site.xml"));
    conf.addResource(new Path(HADOOP_CONF_PATH + "/hdfs-site.xml"));

    FileSystem fs = FileSystem.get(conf);
    System.out.println("file system get from conf: " + fs.toString());
    DistributedFileSystem hdfs = (DistributedFileSystem) DistributedFileSystem.get(conf);

    DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
    for (int i = 0; i < dataNodeStats.length; ++i) {
      System.out.println("****************");
      System.out.println("DataNode_" + i + "_Node:" + dataNodeStats[i].getHostName());
      System.out.println("DataNode_" + i + "_Node:" + dataNodeStats[i].getHost());
      System.out.println("DataNode_" + i + "_Node:" + dataNodeStats[i].getName());
      System.out.println("DataNode_" + i + "_Node:" + dataNodeStats[i].getNetworkLocation());
      System.out.println("DataNode_" + i + "_Node:" + dataNodeStats[i].getXceiverCount());
    }
  }
}
