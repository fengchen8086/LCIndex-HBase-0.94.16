package doTest.tcp;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import doWork.file.LCCHFileMoverClient;
import doWork.file.LCCHFileMoverClient.RemoteStatus;

public class TestLCCHFileMover {

  public static void main(String args[]) throws UnknownHostException, IOException {
    String[] hosts =
        new String[] { "hec-02", "hec-03", "hec-07", "hec-08", "hec-13", "hec-16", "hec-17" };
    String baseDir = "/home/fengchen/softwares/hbase-0.94.16/forcopy";
    Configuration conf = HBaseConfiguration.create();
    File dirDile = new File(baseDir + "/hec");
    if (dirDile.isDirectory()) {
      for (File file : dirDile.listFiles()) {
        for (String host : hosts) {
          LCCHFileMoverClient moverClient = new LCCHFileMoverClient(host, conf);
          String targetName = baseDir + "/" + host + "/" + file.getName();
          if (moverClient.copyRemoteFile(targetName, null, true) == RemoteStatus.SUCCESS) {
            System.out.println("success in moving from host " + host + ", file is: " + targetName);
          }
        }
      }
    }
  }
}
