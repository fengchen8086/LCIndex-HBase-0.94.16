package doWork.jobs;

import java.io.IOException;
import java.util.Queue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;

import doWork.LCCIndexConstant;
import doWork.LCCIndexGenerator;
import doWork.file.LCIndexWriter;

public class FlushJobQueue extends BasicJobQueue {
  private static final FlushJobQueue singleton = new FlushJobQueue();

  public static FlushJobQueue getInstance() {
    return singleton;
  }

  @Override
  public boolean checkJobClass(BasicJob job) {
    return job instanceof FlushJob;
  }

  public FlushJob findJobByRawPath(Path rawPath) {
    FlushJob ret = null;
    synchronized (syncJobsObj) {
      for (BasicJob job : jobs) {
        FlushJob fJob = (FlushJob) job;
        if (rawPath.compareTo(fJob.rawPath) == 0) {
          ret = fJob;
          break;
        }
      }
    }
    return ret;
  }

  public static class FlushJob extends BasicJob {
    private Path rawPath; // tmp Path, used by CommitJob
    private Store store;
    private Queue<KeyValue> queue;
    private TimeRangeTracker tracker;

    public FlushJob(Store store, Queue<KeyValue> keyvalueQueue, Path pathName,
        TimeRangeTracker snapshotTimeRangeTracker) {
      this.store = store;
      this.queue = keyvalueQueue;
      this.rawPath = pathName;
      this.tracker = snapshotTimeRangeTracker;
      if (printForDebug) {
        System.out.println("winter FlushJob construction, rawPath: " + rawPath);
      }
    }

    @Override
    public boolean checkReady() { // flush is always true
      return true;
    }

    @Override
    public boolean correspondsTo(BasicJob j) throws IOException {
      throw new IOException("winter FlushJob.correspondsTo() should not called");
    }

    @Override
    protected void printRelatedJob() {
      System.out.println("winter flush job want to write " + rawPath + ", just waiting, this id: "
          + id);
    }

    @Override
    public void promoteRelatedJobs(boolean processNow) throws IOException {
      // flush has no related jobs, do nothing
    }

    @Override
    public void work() throws IOException {
      long lccFlushStart = System.currentTimeMillis();
      int indexCounter = 0;
      int rawKeyValueCounter = queue.size();
      String desStr =
          store.getHRegion().getTableDesc().getValue(LCCIndexConstant.LC_TABLE_DESC_RANGE_STR);
      LCCIndexGenerator generator = new LCCIndexGenerator(store.lccIndexQualifierType, desStr);
      generator.processKeyValueQueue(queue);
      KeyValue[] indexResults = generator.generatedSortedKeyValueArray();
      indexCounter = indexResults.length;
      LCIndexWriter writer =
          new LCIndexWriter(store, rawPath, generator.getResultStatistic(),
              generator.getLCRangeStatistic(), tracker);
      for (KeyValue kv : indexResults) {
        writer.append(kv);
      }
      writer.close();
      if (printForDebug) {
        System.out.println("winter flush job, flush memstore lcindex cost: "
            + (System.currentTimeMillis() - lccFlushStart) / 1000.0 + " seconds for raw size:"
            + rawKeyValueCounter + " and lcc size: " + indexCounter);
      }

    }
  }

}
