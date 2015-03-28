package doWork.jobs;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import doTestAid.WinterOptimizer;
import doWork.LCCIndexConstant;
import doWork.jobs.CompleteCompactionJobQueue.CompleteCompactionJob;

public class ArchiveJobQueue extends BasicJobQueue {

  private static final ArchiveJobQueue singleton = new ArchiveJobQueue();

  public static ArchiveJobQueue getInstance() {
    return singleton;
  }

  @Override
  public boolean checkJobClass(BasicJob job) {
    return job instanceof ArchiveJob;
  }

  /**
   * delete the old files used by related compaction job
   * @author
   */
  public static class ArchiveJob extends BasicJob {

    private Store store;
    private Collection<StoreFile> compactedFiles;
    private CompleteCompactionJob prevCompleteCompactJob;
    private Path hdfsPath;
    private boolean isMajor;

    public ArchiveJob(Store store, Collection<StoreFile> compactedFiles, Path hdfsPath,
        CompleteCompactionJob completeCompactionJob, boolean isMajor) {
      this.store = store;
      this.compactedFiles = compactedFiles;
      this.hdfsPath = hdfsPath;
      this.prevCompleteCompactJob = completeCompactionJob;
      this.isMajor = isMajor;
      if (printForDebug) {
        System.out.println("winter ArchiveJob construction, hdfsPath: " + hdfsPath);
      }
    }

    @Override
    public boolean checkReady() {
      boolean ret =
          prevCompleteCompactJob == null
              || prevCompleteCompactJob.getStatus() == JobStatus.FINISHED;
      if (ret) {
        if (prevCompleteCompactJob == null) {
          System.out.println("winter archive job ready beacuse prevCompleteCompactJob == null");
        } else {
          System.out
              .println("winter archive job ready beacuse prevCompleteCompactJob has finished");
          prevCompleteCompactJob.printRelatedJob();
        }
      }
      return ret;
    }

    @Override
    protected void printRelatedJob() {
      if (prevCompleteCompactJob == null) {
        System.out.println("winter ArchiveJob.prevCompleteCompactJob is null, just waiting");
      } else {
        System.out.println("winter ArchiveJob is waiting for prevCompleteCompactJob: "
            + prevCompleteCompactJob.getStatus() + ", prevID: " + prevCompleteCompactJob.id
            + ", this id: " + id);
        prevCompleteCompactJob.printRelatedJob();
      }
    }

    @Override
    public boolean correspondsTo(BasicJob j) throws IOException {
      Collection<StoreFile> sfs = ((ArchiveJob) j).compactedFiles;
      if (compactedFiles == null && sfs == null) return true;
      else if (compactedFiles == null || sfs == null) return false;
      else if (compactedFiles.size() != sfs.size()) {
        return false;
      } else {
        for (StoreFile sf1 : sfs) {
          boolean found = false;
          for (StoreFile sf2 : compactedFiles) {
            if (sf1.getPath().equals(sf2.getPath())) {
              found = true;
              break;
            }
          }
          if (!found) return false;
        }
      }
      return true;
    }

    @Override
    public void promoteRelatedJobs(boolean processNow) throws IOException {
      if (prevCompleteCompactJob != null) {
        prevCompleteCompactJob.promoteRelatedJobs(processNow);
      }
    }

    @Override
    public void work() throws IOException {
      // delete local lccindex file
      // hfilePath = /hbase/lcc/AAA/.tmp/BBB/
      // lccLocalHome = /lcc/AAA/f/.lccindex
      FileStatus[] fileStatusList = store.localfs.listStatus(store.lccLocalHome);
      if (fileStatusList == null || fileStatusList.length == 0) {
        if (CommitJobQueue.getInstance().findJobByDestPath(hdfsPath) != null) {
          System.out.println("winter found hdfs from commit job: " + hdfsPath);
        }
        if (CompactJobQueue.getInstance().findJobByDestPath(hdfsPath) != null) {
          System.out.println("winter found hdfs from compact job: " + hdfsPath);
        }
        WinterOptimizer.ThrowWhenCalled("winter archive job, has no index file under: "
            + store.lccLocalHome + ", isMajor: " + isMajor);
      }
      for (FileStatus fileStatus : fileStatusList) {
        for (StoreFile lccToBeCompacted : compactedFiles) {
          // lccToBeCompacted = /hbase/lcc/AAA/f/B1-B3, AAA is always the same
          // lcIdxPath = /hbase/lcc/AAA/f/.lccindex/Q1/Bi. In this loop, Bi is changing
          Path lcIdxPath = new Path(fileStatus.getPath(), lccToBeCompacted.getPath().getName());
          Path lcStatPath =
              new Path(fileStatus.getPath(), lccToBeCompacted.getPath().getName()
                  + LCCIndexConstant.LC_STAT_FILE_SUFFIX);
          if (store.localfs.exists(lcIdxPath)) {
            if (printForDebug) {
              System.out.println("winter now to delete " + lcIdxPath);
            }
            store.localfs.delete(lcIdxPath, true);
            store.localfs.delete(lcStatPath, true);
          } else if (lccToBeCompacted.isReference() && isMajor) {
            // ignore the ref of major
          } else if (isMajor) {
            // major should implement more!
            WinterOptimizer.NeedImplementation("winter need to cancel jobs for major compaction");
          } else {
            WinterOptimizer
                .ThrowWhenCalled("winter archive job, commit has done but missing lcIdxFiles: "
                    + lcIdxPath + ", isMajor: " + isMajor);
          }
          if (printForDebug) {
            System.out.println("winter archive in complete compact " + lcIdxPath + ", hdfs: "
                + lccToBeCompacted.getPath());
          }
        }
      }
    }
  }

}
