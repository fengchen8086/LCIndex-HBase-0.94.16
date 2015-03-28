package doWork;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseFileSystem;
import org.apache.hadoop.hbase.util.FSUtils;

public class LCCIndexDebuggerGiveUP {

  //
  // see how a path is called
  //

  public static TreeMap<String, Integer> hfileMap = new TreeMap<String, Integer>();
  private static int callTimeCounter = 0;

  public static synchronized int updatePath(Path p) {
    ++callTimeCounter;
    int v = 1;
    if (hfileMap.containsKey(p.toString())) {
      v = hfileMap.get(p.toString()) + 1;
    }
    hfileMap.put(p.toString(), v);
    printCallTimeMap();
    return v;
  }

  public static synchronized void printCallTimeMap() {
    System.out
        .println("******* LCCIndexDebugger has been called for " + callTimeCounter + " times");
    for (Entry<String, Integer> entry : hfileMap.entrySet()) {
      System.out.println("LCCIndexDebugger file: " + entry.getKey() + ", times: "
          + entry.getValue());
    }
  }

  //
  // see how a lccindex hfile is called
  //

  public static TreeMap<Path, Path> lccIndexHFileMap = new TreeMap<Path, Path>();
  public static FileSystem hdfs;

  static {
    // hdfs = WinterTestAID.getHDFS();
    hdfs = null;
  }

  public static synchronized int updateAllPossibleLCCHFiles() throws IOException {
    List<Path> listPath =
        listLCCPath(LCCIndexConstant.INDEX_DIR_NAME, LCCIndexConstant.INDEX_DIR_NAME_DEBUG);
    Path debugPath;
    int changes = 0;
    for (Path p : listPath) {
      // p: /hbase/lcc/**/f/.lccindex/name
      debugPath = new Path(p.getParent().getParent(), LCCIndexConstant.INDEX_DIR_NAME_DEBUG);
      if (!hdfs.exists(debugPath)) {
        hdfs.mkdirs(debugPath);
      }
      if (lccIndexHFileMap.containsKey(p)) {
        // todo nothing now
      } else { // now inside, put it in
        if (hdfs.exists(p)) {
          Path newPath = new Path(debugPath, p.getName());
          if (!HBaseFileSystem.renameDirForFileSystem(hdfs, p, newPath)) {
            System.out.println("winter in LCCIndexDebugger, rename file error: " + p);
          } else {
            lccIndexHFileMap.put(p, newPath);
          }
        } else {
          System.out.println("winter in LCCIndexDebugger, lcc hfile should exists but not: " + p);
        }
      }
    }
    return changes;
  }

  public static synchronized void restoreLCCHFiles() throws IOException {
    List<Path> listPath =
        listLCCPath(LCCIndexConstant.INDEX_DIR_NAME_DEBUG, LCCIndexConstant.INDEX_DIR_NAME);
    Path lccPath;
    for (Path p : listPath) {
      // p: /hbase/lcc/**/f/.debuglcc/name
      lccPath = new Path(p.getParent().getParent(), LCCIndexConstant.INDEX_DIR_NAME);
      if (!hdfs.exists(lccPath)) {
        hdfs.mkdirs(lccPath);
      }
      if (hdfs.exists(p)) {
        Path newPath = new Path(lccPath, p.getName());
        if (!HBaseFileSystem.renameDirForFileSystem(hdfs, p, newPath)) {
          System.out.println("winter in LCCIndexDebugger, restore file error: " + p);
        }
      } else {
        System.out.println("winter in LCCIndexDebugger, lcc debug hfile should exists but not: "
            + p);
      }
    }
  }

  public static synchronized Path getMappedLCCPath(Path p) throws IOException {
    updateAllPossibleLCCHFiles();
    if (lccIndexHFileMap.containsKey(p)) {
      return lccIndexHFileMap.get(p);
    }
    return null;
  }

  static int listLCCPathCounter = 0;

  // return all files /hbase/lcc/**/f/.lccindex/name
  private static synchronized List<Path> listLCCPath(String priDir, String bakDir)
      throws IOException {
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
        Path lccDirPath = new Path(fsL2.getPath(), priDir);
        Path debugPath = new Path(fsL2.getPath(), bakDir);
        for (FileStatus fsL3 : statusL3) {
          if (fsL3.getPath().getName().startsWith(".")) {
            continue;
          }
          // now fsL3 is
          // /hbase/lcc/635d2206193eac0ecbba2f5ab2ef58ab/f/4f3c8c126d0e4a5193d5b883630e19c5
          FileStatus[] statusL4 = hdfs.listStatus(fsL3.getPath());
          for (FileStatus fsL4 : statusL4) {
            Path lccHFilePath = new Path(lccDirPath, fsL4.getPath().getName());
            Path debugHFilePath = new Path(debugPath, fsL4.getPath().getName());
            if (hdfs.exists(lccHFilePath)) {
              lccList.add(lccHFilePath);
            } else {
              if (!hdfs.exists(debugHFilePath)) {
                System.out
                    .println("winter error, bot lcc and debug file not exist but the raw is there: "
                        + lccHFilePath);
              }
            }
          }
        }
      }
    }
    return lccList;
  }

  public static void main(String[] args) throws IOException {
    // updateAllPossibleLCCHFiles();
    restoreLCCHFiles();
    List<Path> list =
        listLCCPath(LCCIndexConstant.INDEX_DIR_NAME, LCCIndexConstant.INDEX_DIR_NAME_DEBUG);
    for (Path p : list) {
      System.out.println("path: " + p);
    }
  }
}
