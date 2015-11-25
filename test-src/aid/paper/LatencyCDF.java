package aid.paper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class LatencyCDF {

  class CDF {
    public double[] pivots;
    public double[] values;

    public CDF(int size) {
      pivots = new double[size];
      values = new double[size];
    }

    public void cal() {
      for (int i = values.length - 2; i >= 0; --i) {
        values[i] = values[i + 1] + values[i];
      }
    }
  }

  static String dir = "/home/winter/icpp/gnuplot-scripts/data/test-results";
  static String outPutFile = "/home/winter/icpp/gnuplot-scripts/data/insert-latency-cdf.dat";
  int totalThreads = 0;
  boolean settingTotalThread = true;

  public static void main(String[] args) throws IOException {
    new LatencyCDF().doWork();
  }

  public void doWork() throws IOException {
    File f = new File(dir);
    File[] files = f.listFiles();
    CDF hbase = null, lcc = null, cm = null, cc = null, ir = null;
    for (File file : files) {
      if (file.getName().startsWith("hbase")) {
        hbase = readFile(file);
      } else if (file.getName().startsWith("cc")) {
        cc = readFile(file);
      } else if (file.getName().startsWith("cm")) {
        cm = readFile(file);
      } else if (file.getName().startsWith("ir")) {
        ir = readFile(file);
      } else if (file.getName().startsWith("lcc")) {
        lcc = readFile(file);
      }
    }
    System.out.println("type  HBase CMIndex GCIndex IRIndex LCIndex");
    for (int i = hbase.pivots.length - 1; i >= 0; --i) {
      StringBuilder sb = new StringBuilder();
      sb.append(convert(hbase.pivots[i]));
      sb.append("\t").append(convert(hbase.values[i] / totalThreads));
      sb.append("\t").append(convert(cm.values[i] / totalThreads));
      sb.append("\t").append(convert(cc.values[i] / totalThreads));
      sb.append("\t").append(convert(ir.values[i] / totalThreads));
      sb.append("\t").append(convert(lcc.values[i] / totalThreads));
      System.out.println(sb.toString());
    }
  }

  private String convert(double d) {
    return String.format("%.02f", d);
  }

  public CDF readFile(File file) throws IOException {
    String PARSING_STR_1 = "latency report part:";
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    CDF cdf = null;
    while ((line = br.readLine()) != null) {
      if (line.indexOf(PARSING_STR_1) > -1) {
        line = line.substring(line.indexOf(PARSING_STR_1) + PARSING_STR_1.length() + 3);
        if (settingTotalThread) ++totalThreads;
        cdf = updateValue(cdf, line);
      }
    }
    br.close();
    settingTotalThread = false;
    cdf.cal();
    return cdf;
  }

  public CDF updateValue(CDF cdf, String line) {
    String parts[] = line.split(",");
    if (cdf == null) {
      cdf = new CDF(parts.length);
    }
    for (int i = 0; i < parts.length; ++i) {
      // part = "[10.0->0.000%]";
      parts[i] = parts[i].trim();
      String[] ones = parts[i].substring(1, parts[i].length() - 2).split("->");
      cdf.pivots[i] = Double.valueOf(ones[0]);
      cdf.values[i] += Double.valueOf(ones[1]);
    }
    return cdf;
  }
}
