package aid.paper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class JobCDF {

  final double[] pivots = new double[] { 180, 150, 120, 100, 80, 65, 50, 40, 25, 15, 10, 8, 6, 5,
      4, 3.5, 3, 2.5, 2, 1.5, 1, 0.8, 0.5, 0.3, 0.2, 0.1, 0 };
  int[] flushValues = new int[pivots.length];
  int[] commitValues = new int[pivots.length];
  int[] compactValues = new int[pivots.length];
  int[] completeValues = new int[pivots.length];
  int[] archiveValues = new int[pivots.length];

  double totalFlushTime = 0;
  double totalCommitTime = 0;
  double totalCompactTime = 0;
  double totalCompleteTime = 0;
  double totalArchiveTime = 0;
  double totalRemoteTime = 0;

  int totalFlushJob = 0;
  int totalCommitJob = 0;
  int totalCompactJob = 0;
  int totalCompleteJob = 0;
  int totalArchiveJob = 0;
  int totalRemoteJob = 0;

  static String dir = "/home/winter/icpp/gnuplot-scripts/data/lcc-out";

  public static void main(String[] args) throws IOException {
    new JobCDF().work();
  }

  private void work() throws IOException {
    File f = new File(dir);
    for (File file : f.listFiles()) {
      if (file.getName().indexOf("base-fengchen-regionserver") > 0) {
        parseFile(file);
      }
    }
    System.out.println("type  Flush Commit  Compact Complete Archive");
    calValues();
    for (int i = pivots.length - 1; i >= 0; --i) {
      StringBuilder sb = new StringBuilder();
      sb.append(convert(pivots[i]));
      sb.append("\t").append(convert(100.0 * flushValues[i] / totalFlushJob));
      sb.append("\t").append(convert(100.0 * commitValues[i] / totalCommitJob));
      sb.append("\t").append(convert(100.0 * compactValues[i] / totalCompactJob));
      sb.append("\t").append(convert(100.0 * completeValues[i] / totalCompleteJob));
      sb.append("\t").append(convert(100.0 * archiveValues[i] / totalArchiveJob));
      System.out.println(sb.toString());
    }

    System.out.println(totalFlushTime / totalFlushJob);
    System.out.println(totalCommitTime / totalCommitJob);
    System.out.println(totalCompactTime / totalCommitJob);
    System.out.println(totalCompleteTime / totalCompleteJob);
    System.out.println(totalArchiveTime / totalArchiveJob);
  }

  private String convert(double d) {
    return String.format("%.02f", d);
  }

  private void parseFile(File file) throws IOException {
    String SUFFIX_STR = "seconds before start working";
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    while ((line = br.readLine()) != null) {
      if (line.indexOf(SUFFIX_STR) > -1) {
        String parts[] = line.split("cost");
        double v = Double.valueOf(parts[1].substring(0, parts[1].indexOf(SUFFIX_STR)).trim());
        if (parts[0].trim().endsWith("FlushJob")) {
          totalFlushJob++;
          totalFlushTime += v;
          updateValues(flushValues, v);
        } else if (parts[0].trim().endsWith("CommitJob")) {
          totalCommitJob++;
          totalCommitTime += v;
          updateValues(commitValues, v);
        } else if (parts[0].trim().endsWith("CompactJob")) {
          totalCompactJob++;
          totalCompactTime += v;
          updateValues(compactValues, v);
        } else if (parts[0].trim().endsWith("CompleteCompactionJob")) {
          totalCompleteJob++;
          totalCompleteTime += v;
          updateValues(completeValues, v);
        } else if (parts[0].trim().endsWith("ArchiveJob")) {
          totalArchiveJob++;
          totalArchiveTime += v;
          updateValues(archiveValues, v);
        } else {
          System.out.println(line);
        }
      }
    }
    br.close();
  }

  void updateValues(int[] values, double v) {
    for (int i = 0; i < pivots.length; ++i) {
      if (pivots[i] <= v) {
        values[i] += 1;
        break;
      }
    }
  }

  private void calValues() {
    calValue(flushValues);
    calValue(commitValues);
    calValue(compactValues);
    calValue(completeValues);
    calValue(archiveValues);
  }

  private void calValue(int[] values) {
    for (int i = values.length - 2; i >= 0; --i) {
      values[i] = values[i + 1] + values[i];
    }
  }

}
