package doTest.put;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.hbase.index.client.DataType;

import doTest.put.PutTestConstants.CF_INFO;

public class GenerateDataIntoFile {

  private void work(int recordNumber, String dataFileName) throws IOException {
    Random random = new Random();
    BufferedWriter writer =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dataFileName)));
    StringBuilder sb = null;
    // rowkey \t qualifier:value1 \t qualifier: value2 ...
    for (int i = 0; i < recordNumber; ++i) {
      sb = new StringBuilder();
      sb.append(String.format("1%05d", i));
      for (CF_INFO ci : PutTestConstants.getCFInfo()) {
        if (ci.type == DataType.STRING && ci.isIndex == false) {
          continue;
        }
        sb.append("\t");
        sb.append(ci.qualifier);
        sb.append(":");
        sb.append(String.valueOf(random.nextInt(recordNumber)));
      }
      sb.append("\n");
      writer.write(sb.toString());
    }
    writer.close();
  }

  public static void usage() {
    System.out.println("GenerateDataIntoFile recordnumber(int) path_to_write_data");
  }

  public static void main(String[] args) throws NumberFormatException, IOException {
    if (args.length < 2) {
      usage();
      return;
    }
    new GenerateDataIntoFile().work(Integer.valueOf(args[0]), args[1]);
  }
}
