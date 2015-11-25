package doTest.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class TraverseHDFS {

  public static String HADOOP_CONF_PATH = "/home/winter/softwares/hadoop-1.0.4/conf";
  public static int totalLCCCounter = 0;
  public static int totalRawCounter = 0;
  public static final String want =
      "/home/fengchen/data/lccindex/tpch_lcc/03ce943b4994200e07662ca5a819fdf8/f/.lccindex/date";
  public static final String prefix = "/home/fengchen/data/lccindex/";
  private static boolean printLCC = false;
  private static boolean printRaw = false;

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
    new TraverseHDFS().work(hadoop_conf_path, baseDir);
  }

  private void work(String hadoop_conf_path, String baseDir) throws IOException {
    Configuration conf = getConf(hadoop_conf_path);
    DistributedFileSystem hdfs = (DistributedFileSystem) DistributedFileSystem.get(conf);
    if (baseDir == null) baseDir = "/hbase/lcc";
    Path root = new Path(baseDir);
    traverse(hdfs, root);
    System.out.println("total raw file on hdfs: " + totalRawCounter);
    System.out.println("total lccindex file on hdfs: " + totalLCCCounter);
  }

  private void traverse(DistributedFileSystem hdfs, Path home) throws IOException {
    // if(hdfs.get)
    FileStatus homeFS = hdfs.getFileStatus(home);
    if (homeFS.isDir()) {
      // System.out.println("Dir: " + home);
      FileStatus[] files = hdfs.listStatus(home);
      if (files != null) {
        for (FileStatus file : files) {
          traverse(hdfs, file.getPath());
        }
      }
    } else if (home.toString().indexOf(".lccindex") == -1) {
      if (home.getName().length() > 25) {
        ++totalRawCounter;
        if (printRaw) {
          System.out.println(home);
        }
      }
      return;
    } else {
      ++totalLCCCounter;
      if (printLCC) {
        // System.out.println(home + " block size: " + homeFS.getBlockSize());
        BlockLocation[] b_locations = hdfs.getFileBlockLocations(homeFS, 0, homeFS.getBlockSize());
        for (BlockLocation bl : b_locations) {
          String names[] = bl.getNames();
          String hosts[] = bl.getHosts();
          System.out.println("blocks are located in " + names.length + " hosts");
          for (int i = 0; i < names.length; ++i) {
            System.out.println(names[i] + " locates in: " + hosts[i]);
          }
        }
      }
    }
  }
}
