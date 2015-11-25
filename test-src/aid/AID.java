package aid;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import doWork.LCCIndexConstant;
import doWork.LCStatInfo;

public class AID {

  enum x {
    hahh, nono;
  }

  public static void main(String[] args) throws IOException, ParseException {
    double d = 3.1415926535;
    System.out.println(String.format("%.3f%%", d * 100));
  }

  public static void runA() throws IOException {
    String str = getStatString();
    System.out.println(str);
    List<LCStatInfo> list = LCStatInfo.parseStatString(str);
    byte[] qualifier = Bytes.toBytes("date");
    byte[] v1 = Bytes.toBytes((long) 19920162);
    byte[] v2 = Bytes.toBytes((long) 19920162);
    for (LCStatInfo stat : list) {
      System.out.println(stat);
      if (Bytes.equals(qualifier, stat.getQualifier())) {
        stat.updateRange(v1);
        stat.updateRange(v2);
      }
      System.out.println(stat.getValuedString());
    }
  }

  public static String getStatString() throws IOException {
    String dataFileName = "/home/winter/softwares/hbase-0.94.16/stat.dat";
    BufferedReader br = new BufferedReader(new FileReader(dataFileName));
    String line;
    StringBuilder sb = new StringBuilder();
    while ((line = br.readLine()) != null) {
      sb.append(line).append(LCCIndexConstant.LCC_TABLE_DESC_RANGE_DELIMITER);
    }
    br.close();
    return sb.toString();
  }
}
