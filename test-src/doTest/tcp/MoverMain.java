package doTest.tcp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class MoverMain {

  public static void main(String[] args) throws IOException, InterruptedException {
    new MoverMain().work(args);
  }

  private void startServer() throws IOException, InterruptedException {
    Configuration conf = getConf();
    ServerMain server = new ServerMain(conf);
    Thread threadS = new Thread(server);
    threadS.start();
    threadS.join();
  }

  private void startClient(String host, String fileName) throws IOException {
    ClientMain client = new ClientMain(host, getConf());
    client.containsFileAndCopy(fileName);
  }

  private static Configuration getConf() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    String fileName = "/home/winter/softwares/hbase-0.94.16/conf/winter-assign";
    File file = new File(fileName);
    if (file.exists()) {
      System.out.println("winter load newly defined values from file: " + fileName);
      parseMannuallyAssignedFile(conf, fileName);
    }
    return conf;
  }

  private void work(String[] args) throws IOException, InterruptedException {
    if (args.length >= 1) {
      if (args[0].equals("server")) {
        startServer();
      } else if (args[0].equals("client") && args.length >= 3) {
        startClient(args[1], args[2]);
      }
    }
  }

  public static void parseMannuallyAssignedFile(Configuration conf, String file) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    String parts[] = null;
    while ((line = br.readLine()) != null) {
      if (line.startsWith("#")) {
        System.out.println("skip line: " + line);
        continue;
      }
      parts = line.split("\t");
      if (parts.length != 2) {
        System.out.println("line error: " + line);
        continue;
      }
      System.out.println("setting line: " + line);
      conf.set(parts[0], parts[1]);
    }
    br.close();
  }
}
