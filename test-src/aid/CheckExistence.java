package aid;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class CheckExistence {

  private Configuration conf;
  private DistributedFileSystem hdfs;

  private void work(String hadoopConfDir, String targetName) throws IOException {
    conf = new Configuration();
    conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
    conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));
    conf.set("fs.default.name", "hdfs://hec-14:21345");
    hdfs = (DistributedFileSystem) DistributedFileSystem.get(conf);
    System.out.println(hdfs.exists(new Path(targetName)));
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out.println("CheckLCFiles hadoopConfDir wantCheckedName");
      return;
    }
    new CheckExistence().work(args[0], args[1]);
  }
}
