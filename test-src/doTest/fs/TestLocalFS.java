package doTest.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class TestLocalFS {

  static Configuration conf = new Configuration();
  static {
    conf.addResource(new Path("/home/winter/softwares/hadoop-1.0.4/conf/core-site.xml"));
    conf.addResource(new Path("/home/winter/softwares/hadoop-1.0.4/conf/hdfs-site.xml"));
  }

  public static void main(String[] args) throws Exception {
    testLocalFs();
  }

  public static void testLocalFs() throws IOException {
    LocalFileSystem fs = LocalFileSystem.getLocal(conf);
    System.out.println(fs);
    String testDir = "/home/winter/temp/temp";
    String innerDir = "test";
    FileStatus[] statusArray = fs.listStatus(new Path(testDir));
    if (statusArray == null || statusArray.length == 0) {
      Path innerDirPath = new Path(new Path(testDir), innerDir);
      Path innerFilePath = new Path(new Path(testDir), "hdfs-site");
      fs.mkdirs(innerDirPath);
      fs.copyFromLocalFile(new Path("/home/winter/hdfs-site.xml"), innerFilePath);
      fs.setReplication(innerFilePath, (short) 3);
    }
    for (FileStatus status : statusArray) {
      if (status.isDir()) {
        System.out.println("winter file is a dir: " + status.getPath());
      } else {
        System.out.println("winter file is a file: " + status.getPath().getName());
      }
    }
  }
}
