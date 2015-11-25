package aid;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class CheckLCFiles {

  private String LC_ROOT_DIR;
  private Configuration conf;
  private DistributedFileSystem hdfs;
  private String HABSE_HDFS_PREFIX = "/hbase/";
  private long totalIFileSize = 0;
  private long totalIFileNumber = 0;

  private void work(String hadoopConfDir, String lccRootDir) throws IOException {
    LC_ROOT_DIR = lccRootDir;
    conf = new Configuration();
    conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
    conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));
    conf.set("fs.default.name", "hdfs://hec-14:21345");
    hdfs = (DistributedFileSystem) DistributedFileSystem.get(conf);
    File root = new File(lccRootDir);
    if (root.exists()) {
      traverse(root);
      if (!root.exists()) {
        root.mkdirs();
      }
    }

    System.out.println("on server " + InetAddress.getLocalHost().getHostName() + ",there are "
        + totalIFileNumber + " IFiles, total size: " + totalIFileSize + ", in MB: "
        + totalIFileSize / 1024 / 1024);
  }

  private void traverse(File root) throws IOException {
    if (root.isDirectory()) {
      for (File f : root.listFiles()) {
        traverse(f);
      }
      if (root.listFiles() == null || root.listFiles().length == 0) {
        System.out.println("delete dir: " + root);
        root.delete();
      }
    } else {
      if (!root.getName().endsWith("-stat")) {
        String hdfsPath = getHFilePath(root);
        if (!existOnHDFS(hdfsPath)) {
          File sFile = new File(root.getAbsolutePath() + "-stat");
          if (sFile.exists()) {
            sFile.delete();
          }
          System.out.println("delete IFile: " + root + ", because hdfs not exist: " + hdfsPath);
          root.delete();
        } else {
          ++totalIFileNumber;
          totalIFileSize += root.length();
        }
      }
    }
  }

  private boolean existOnHDFS(String hfilePath) throws IOException {
    return hdfs.exists(new Path(hfilePath));
  }

  private String getHFilePath(File iFile) {
    String name = iFile.getParentFile().getParentFile().getParentFile().getAbsolutePath();
    name = name.substring(LC_ROOT_DIR.length());
    if (!name.startsWith("/")) {
      name = "/" + name;
    }
    return HABSE_HDFS_PREFIX + "/" + name + "/" + iFile.getName();
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out.println("CheckLCFiles hadoopConfDir lccRootDir");
      return;
    }
    new CheckLCFiles().work(args[0], args[1]);
  }
}
