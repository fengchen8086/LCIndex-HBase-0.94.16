package doTest.fs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class TraverseLocalFS {

  public static String HADOOP_CONF_PATH = "/home/winter/softwares/hadoop-1.0.4/conf";
  public static int totalLCCCounter = 0;

  private static Configuration getConf(String hadoop_conf_path) {
    Configuration conf = new Configuration();
    if (hadoop_conf_path == null) {
      hadoop_conf_path = HADOOP_CONF_PATH;
    } else {
      conf.set("fs.default.name", "hdfs://lingcloud25:9000");
    }
    System.out.println("hadoop_conf_path is: " + hadoop_conf_path);
    conf.addResource(new Path(hadoop_conf_path + "/core-site.xml"));
    conf.addResource(new Path(hadoop_conf_path + "/hdfs-site.xml"));
    return conf;
  }

  public static void main(String[] args) throws IOException {
    String hadoop_conf_path = null;
    String baseDir = null;
    if (args.length >= 1) {
      hadoop_conf_path = args[0];
    }
    if (args.length >= 2) {
      baseDir = args[1];
    }
    new TraverseLocalFS().work(hadoop_conf_path, baseDir);
  }

  private void work(String hadoop_conf_path, String baseDir) throws IOException {
    Configuration conf = getConf(hadoop_conf_path);
    FileSystem localfs = LocalFileSystem.get(conf);
    System.out.println(localfs.getClass().getName());
    if (baseDir == null) baseDir = "/home/winter/temp/data/lccindex";
    traverse(new File(baseDir));
    System.out.println("total hdfs file: " + totalLCCCounter);
  }

  private void traverse(File home) throws IOException {
    // if(hdfs.get)
    if (home.isDirectory()) {
      File[] files = home.listFiles();
      if (files != null) {
        for (File file : files) {
          traverse(file);
        }
      }
    } else {
      if (home.toString().indexOf(".lccindex") == -1) {
        return;
      }
      ++totalLCCCounter;
      System.out.println("local file: " + home.getAbsolutePath());
    }
  }
}
