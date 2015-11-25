package doTest.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.util.FSUtils;

import doWork.LCCIndexConstant;

public class WinterTestAID {

  public static FileSystem getHDFS() throws IOException {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/winter/softwares/hadoop-1.0.4/conf/core-site.xml"));
    conf.addResource(new Path("/home/winter/softwares/hadoop-1.0.4/conf/hdfs-site.xml"));
    FileSystem hdfs = FileSystem.get(conf);
    return hdfs;
  }

  public static void readHFile(Configuration hbaseConf, Path hfilePath) throws IOException {
    CacheConfig tmpCacheConfig = new CacheConfig(hbaseConf);
    FileSystem hdfs = getHDFS();
    if (!hdfs.exists(hfilePath)) {
      System.out.println("WinterTestAID file not exists: " + hfilePath);
    } else {
      System.out.println("WinterTestAID reading lccindex hfile: " + hfilePath);
      StoreFile sf = new StoreFile(hdfs, hfilePath, hbaseConf, tmpCacheConfig, BloomType.NONE, null);
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

  public static void readHFile(Configuration hbaseConf, String hfilePath) throws IOException {
    readHFile(hbaseConf, new Path(hfilePath));
  }

  //
  public static List<Path> listLCCPath() throws IOException {
    FileSystem hdfs = WinterTestAID.getHDFS();
    List<Path> lccList = new ArrayList<Path>();
    Path hbasePath = new Path("/hbase/lcc");
    if (!hdfs.exists(hbasePath)) return lccList;
    FileStatus[] statusL1 = FSUtils.listStatus(hdfs, hbasePath, null);
    for (FileStatus fsL1 : statusL1) {
      if (fsL1.getPath().getName().startsWith(".")) {
        continue;
      }
      // now fsL1 is: localhost:9000/hbase/lcc/635d2206193eac0ecbba2f5ab2ef58ab/
      FileStatus[] statusL2 = hdfs.listStatus(fsL1.getPath());
      for (FileStatus fsL2 : statusL2) {
        if (fsL2.getPath().getName().startsWith(".")) {
          continue;
        }
        // now fsL2 is: localhost:9000/hbase/lcc/635d2206193eac0ecbba2f5ab2ef58ab/f
        FileStatus[] statusL3 = hdfs.listStatus(fsL2.getPath());
        Path lccDirPath = new Path(fsL2.getPath(), ".lccindex");
        for (FileStatus fsL3 : statusL3) {
          if (fsL3.getPath().getName().startsWith(".")) {
            continue;
          }
          // now fsL3 is
          // /hbase/lcc/635d2206193eac0ecbba2f5ab2ef58ab/f/4f3c8c126d0e4a5193d5b883630e19c5
          FileStatus[] statusL4 = hdfs.listStatus(fsL3.getPath());
          for (FileStatus fsL4 : statusL4) {
            Path lccHFilePath = new Path(lccDirPath, fsL4.getPath().getName());
            System.out.println(lccHFilePath);
            if (hdfs.exists(lccHFilePath)) {
              lccList.add(lccHFilePath);
            } else {
              System.out.println("winter error, lccindex file not exist but the raw is there: "
                  + lccHFilePath);
            }
          }
        }
      }
    }
    return lccList;
  }
}
