package doMultiple;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.hbase.index.client.DataType;

import doTest.put.PutTestConstants;
import doTest.put.PutTestConstants.CF_INFO;

public class GenerateDataIntoFileMultiple {

  final String GENERATED_FORMAT = "%012d";

  private void work(int recordNumber, String dataFileName, int threadNumber) throws IOException {
    int span = recordNumber / threadNumber;
    DataWriter[] writers = new DataWriter[threadNumber];
    Thread[] threads = new Thread[threadNumber];
    for (int i = 0; i < threadNumber; ++i) {
      writers[i] = new DataWriter(i * span, (i + 1) * span, recordNumber, i, dataFileName);
      threads[i] = new Thread(writers[i]);
      threads[i].start();
    }
    for (int i = 0; i < threadNumber; ++i) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public static void usage() {
    System.out.println("GenerateDataIntoFile recordnumber(int) path_to_write_data thread_number");
  }

  public static void main(String[] args) throws NumberFormatException, IOException {
    if (args.length < 3) {
      usage();
      return;
    }
    new GenerateDataIntoFileMultiple().work(Integer.valueOf(args[0]), args[1],
      Integer.valueOf(args[2]));
  }

  class DataWriter implements Runnable {

    // [Start, Stop)
    private int start;
    private int stop;
    private String fileName;
    private int recordNumber;

    public DataWriter(int start, int stop, int recordNumber, int id, String filePrefix) {
      this.start = start;
      this.stop = stop;
      this.recordNumber = recordNumber;
      fileName = MultipleConstants.getFileName(filePrefix, id);
    }

    @Override
    public void run() {
      try {
        writeFile();
      } catch (IOException e) {
        e.printStackTrace();
      }
      System.out.println("write from " + start + " to " + stop + " done");
    }

    private void writeFile() throws IOException {
      Random random = new Random();
      BufferedWriter writer =
          new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)));
      StringBuilder sb = null;
      // rowkey \t qualifier:value1 \t qualifier: value2 ...
      for (int i = start; i < stop; ++i) {
        sb = new StringBuilder();
        sb.append(String.format(GENERATED_FORMAT, i));
        for (CF_INFO ci : PutTestConstants.getCFInfo()) {
          if (ci.type == DataType.STRING || ci.isIndex == false) {
            continue;
          }
          sb.append("\t");
          sb.append(ci.qualifier);
          sb.append(":");
          sb.append(String.valueOf(random.nextInt(recordNumber * ci.scale)));
        }
        sb.append("\n");
        writer.write(sb.toString());
      }
      writer.close();
    }
  }
}
