package doWork.file;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.Bytes;

import doWork.LCCIndexConstant;
import doWork.LCCIndexGenerator;
import doWork.LCStatInfo;

public class LCIndexWriter {
  private final Path basePath;
  private TreeMap<byte[], StoreFile.Writer> indexWriters = new TreeMap<byte[], StoreFile.Writer>(
      Bytes.BYTES_COMPARATOR);
  private List<LCStatInfo> rangeInfoList;

  // raw pathName is: /hbase/AAA/xxx/.tmp/BBB,
  // writerBaseDir = /hbase/lcc/AAA/.tmp/BBB.lccindex
  // file /hbase/lcc/AAA/.tmp/BBB.lccindex/Q1-Q4 indicates a index file for qualifier Q1/Q4
  public LCIndexWriter(Store store, Path rawHFile, Map<byte[], Integer> statistic,
      List<LCStatInfo> rangeInfoList, TimeRangeTracker tracker) throws IOException {
    this.rangeInfoList = rangeInfoList;
    if (store.localfs != null) {
      basePath = store.getLocalBaseDirByFileName(rawHFile);
      if (!store.localfs.exists(basePath)) { // create dir
        store.localfs.mkdirs(basePath);
      }
    } else {
      basePath = store.mWinterGetLCCIndexFilePathFromHFilePathInTmp(rawHFile);
    }
    for (Entry<byte[], Integer> entry : statistic.entrySet()) {
      if (entry.getValue() > 0) {
        // targetPath = /hbase/lcc/AAA/.tmp/BBB.lccindex/Q1-Q4
        Path targetPath = new Path(basePath, Bytes.toString(entry.getKey()));
        StoreFile.Writer writer = null;
        if (store.localfs != null) {
          writer =
              store.mWinterCreateWriterInLocalTmp(entry.getValue(), store.family.getCompression(),
                false, true, targetPath);
          writeStatInfo(store.localfs, basePath, Bytes.toString(entry.getKey()));
        } else {
          writer =
              store.mWinterCreateWriterInTmp(entry.getValue(), store.family.getCompression(),
                false, true, targetPath);
          writeStatInfo(store.fs, basePath, Bytes.toString(entry.getKey()));
        }
        if (tracker != null) {
          writer.setTimeRangeTracker(tracker);
        }
        indexWriters.put(entry.getKey(), writer);
      } else {
        System.out.println("winter ignore cf: " + Bytes.toString(entry.getKey())
            + " bacause it contains " + entry.getValue() + " rows");
      }
    }
  }

  private void writeStatInfo(FileSystem fs, Path basePath, String qualifierName) throws IOException {
    for (LCStatInfo stat : rangeInfoList) {
      if (stat.getName().equals(qualifierName)) {
        Path pt = new Path(basePath, qualifierName + LCCIndexConstant.LC_STAT_FILE_SUFFIX);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
        if (stat.isSet()) {
          for (long l : stat.getSetValues()) {
            bw.write(l + "\n");
          }
        } else {
          for (long l : stat.getCounts()) {
            bw.write(l + "\n");
          }
        }
        bw.close();
        return;
      }
    }
  }

  public Path getBaseDirPath() {
    return this.basePath;
  }

  public void append(KeyValue kv) throws IOException {
    byte[] rawQualifier =
        Bytes.toBytes(LCCIndexGenerator.mWinterRecoverLCCQualifier_Str(Bytes.toString(kv
            .getFamily())));
    if (!indexWriters.containsKey(rawQualifier)) {
      System.out.println("winter error, try to write indexed value but writer not exists");
    } else {
      indexWriters.get(rawQualifier).append(kv);
    }
  }

  public void close() throws IOException {
    close(false);
  }

  public void close(boolean print) throws IOException {
    for (Entry<byte[], StoreFile.Writer> entry : indexWriters.entrySet()) {
      entry.getValue().close();
      if (print) {
        System.out.println("winter flushing memstore to file: " + entry.getValue().getPath());
      }
    }
  }

  // /home/winter/temp/data/lccindex/tpch_lcc/.lctmp/d6c3f4e4ad5149aa8f98ce2bae815460.lccindex/priority
  // to
  // /home/winter/temp/data/lccindex/tpch_lcc/207525d39bf02b403759fbc13af3c0ab/f/.lccindex/priority/d6c3f4e4ad5149aa8f98ce2bae815460
}
