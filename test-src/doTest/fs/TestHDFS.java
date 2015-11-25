package doTest.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class TestHDFS {

  public static String HADOOP_CONF_PATH = "/home/winter/softwares/hadoop-1.0.4/conf";


  public static void main(String[] args) throws Exception {
    getHDFSNodes();
    // getFileLocal();
  }

  public static void printDataNodeInfo(String hadoop_conf_path) throws IOException,
      InterruptedException {
    if (hadoop_conf_path == null) hadoop_conf_path = HADOOP_CONF_PATH;
    System.out.println("use hadoop_conf_path: " + hadoop_conf_path);
    Configuration conf = new Configuration();
    conf.addResource(new Path(HADOOP_CONF_PATH + "/core-site.xml"));
    conf.addResource(new Path(HADOOP_CONF_PATH + "/hdfs-site.xml"));

    FileSystem fs = FileSystem.get(conf);
    System.out.println("file system get from conf: " + fs);
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

  public static void getHDFSNodes() throws Exception {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/winter/softwares/hadoop-1.0.4/conf/core-site.xml"));
    conf.addResource(new Path("/home/winter/softwares/hadoop-1.0.4/conf/hdfs-site.xml"));

    FileSystem fs = FileSystem.get(conf);
    DistributedFileSystem hdfs = (DistributedFileSystem) fs;

    DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
    for (int i = 0; i < dataNodeStats.length; ++i) {
      System.out.println("DataNode_" + i + "_Node:" + dataNodeStats[i].getHostName());
    }
  }
}
