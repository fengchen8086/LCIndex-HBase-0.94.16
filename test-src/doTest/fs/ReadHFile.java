package doTest.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;

import doWork.LCCIndexConstant;

public class ReadHFile {
  public static void main(String args[]) throws IOException {
    if (args.length != 4) {
      System.out.println("ReadHFile local/HDFS TARGET_HDFS_DIR");
      return;
    }
    new ReadHFile().work(args[0], args[1], args[2], args[3]);
  }

  String runningConfFileName = "/home/winter/temp/lcc/check-conf/runningconf.dat";

  private void work(String hadoopConfPath, String hbaseConfPath, String fsStr, String fileName)
      throws IOException {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.default.name", "hdfs://lingcloud25:9000");
    System.out.println("use hadoop_conf_path: " + hadoopConfPath);
    hadoopConf.addResource(new Path(hadoopConfPath + "/core-site.xml"));
    hadoopConf.addResource(new Path(hadoopConfPath + "/hdfs-site.xml"));

    Configuration hbaseConf = HBaseConfiguration.create();
    hbaseConf.addResource(hbaseConfPath + "/hbase-site.xml");

    readHFile(hadoopConf, hbaseConf, fsStr, fileName);
  }

  private void readHFile(Configuration hadoopConf, Configuration hbaseConf, String fsStr,
      String fileName) throws IOException {
    CacheConfig tmpCacheConfig = new CacheConfig(hbaseConf);
    FileSystem fs = null;
    if (fsStr.equalsIgnoreCase("local")) {
      fs = LocalFileSystem.getLocal(hadoopConf);
    } else {
      fs = FileSystem.get(hadoopConf);
    }
    Path path = new Path(fileName);
    if (!fs.exists(path)) {
      System.out.println("WinterTestAID file not exists: " + path);
    } else {
      System.out.println("WinterTestAID reading lccindex hfile: " + path);
      StoreFile sf = new StoreFile(fs, path, hbaseConf, tmpCacheConfig, BloomType.NONE, null);
      Reader reader = sf.createReader();
      System.out.println("WinterTestAID store file attr: " + sf.mWinterGetAttribute());
      StoreFileScanner sss = reader.getStoreFileScanner(false, false);
      sss.seek(KeyValue.LOWESTKEY);
      System.out.println("WinterTestAID store peek value: "
          + LCCIndexConstant.mWinterToPrint(sss.peek()));
      KeyValue kv;
      int counter = 0, printInterval = 1, totalSize = 0;
      while ((kv = sss.next()) != null) {
        if (counter == 0) {
          counter = printInterval;
          System.out
              .println("WinterTestAID hfile keyvalue: " + LCCIndexConstant.mWinterToPrint(kv));
        }
        --counter;
        ++totalSize;
      }
      sss.close();
      reader.close(false);
      System.out.println("WinterTestAID total size: " + totalSize);
      System.out.println("WinterTestAID winter inner mWinterGetScannersForStoreFiles start: "
          + LCCIndexConstant.convertUnknownBytes(reader.getFirstKey()));
    }
  }
}
