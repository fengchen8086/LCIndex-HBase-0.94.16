package doWork.jobs;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseFileSystem;
import org.apache.hadoop.hbase.regionserver.Store;

import doTestAid.WinterOptimizer;
import doWork.LCCIndexConstant;
import doWork.jobs.CompactJobQueue.CompactJob;
import doWork.jobs.CompactJobQueue.RebuildCompactJob;

public class CompleteCompactionJobQueue extends BasicJobQueue {

  private static final CompleteCompactionJobQueue singleton = new CompleteCompactionJobQueue();

  public static CompleteCompactionJobQueue getInstance() {
    return singleton;
  }

  @Override
  public boolean checkJobClass(BasicJob job) {
    return job instanceof CompleteCompactionJob;
  }

  public CompleteCompactionJob findJobByDestPath(Path destPath) {
    synchronized (syncJobsObj) {
      for (BasicJob job : jobs) {
        CompleteCompactionJob fJob = (CompleteCompactionJob) job;
        if (destPath.compareTo(fJob.solidHDFSPath) == 0) {
          return fJob;
        }
      }
    }
    return null;
  }

  /**
   * move the new written file from temp path to online path && relies on minor compaction job!
   * @author
   */
  public static class CompleteCompactionJob extends BasicJob {
    private Store store;
    private Path compactedFile; // a tmp file path
    protected Path solidHDFSPath; // solid Path

    private CompactJob prevCompactJob;

    public CompleteCompactionJob(Store store, Path compactedFile, CompactJob minorCompactJob) {
      this.store = store;
      this.compactedFile = compactedFile;
      this.prevCompactJob = minorCompactJob;
      this.solidHDFSPath = new Path(store.homedir, compactedFile.getName());
      String str =
          prevCompactJob == null ? "null" : String
              .valueOf(prevCompactJob instanceof RebuildCompactJob);
      if (printForDebug) {
        System.out.println("winter CompleteCompactionJob construction, compactedFile: "
            + compactedFile + ", solid path: " + solidHDFSPath
            + ", prevCompactJib is RebuildCompactJob: " + str);
      }
    }

    @Override
    public boolean checkReady() {
      boolean ret = prevCompactJob == null || prevCompactJob.getStatus() == JobStatus.FINISHED;
      if (ret) {
        if (prevCompactJob == null) {
          System.out.println("winter CompleteCompactionJob ready beacuse prevCompac job is null");
        } else {
          System.out
              .println("winter CompleteCompactionJob ready beacuse prevCompac job is finished, relatedFlushJob id: "
                  + prevCompactJob.id);
          prevCompactJob.printRelatedJob();
        }
      }
      return ret;
    }

    @Override
    protected void printRelatedJob() {
      if (prevCompactJob == null) {
        System.out.println("winter CompleteCompactionJob.prevCompactJob is null, just waiting");
      } else {
        System.out.println("winter CompleteCompactionJob is waiting for prevCompactJob: "
            + prevCompactJob.getStatus() + ", this id: " + id);
        prevCompactJob.printRelatedJob();
      }
    }

    @Override
    public boolean correspondsTo(BasicJob j) throws IOException {
      // CompleteCompactionJob only care about the compactedFile
      return this.compactedFile.equals(((CompleteCompactionJob) j).compactedFile);
    }

    @Override
    public void promoteRelatedJobs(boolean processNow) throws IOException {
      if (prevCompactJob != null) {
        prevCompactJob.promoteRelatedJobs(processNow);
      }
    }

    @Override
    public void work() throws IOException {
      Path lccLocalTmpPath = store.getLocalBaseDirByFileName(compactedFile);
      // dir lccLocalPath has already got files from compact
      FileStatus[] fileStatusArray = store.localfs.listStatus(lccLocalTmpPath);
      if (fileStatusArray != null) {
        for (FileStatus fileStatus : fileStatusArray) {
          // fileStatus = /hbase/lcc/AAA.tmp/BBB.lccindex/qualifier(-stat)
          // lccLocalHome = /lcc/AAA/f/.lccindex
          if (fileStatus.getPath().getName().endsWith(LCCIndexConstant.LC_STAT_FILE_SUFFIX)) {
            // fileStatus = /lcc/AAA/.tmp/.lccindex/qualifier-stat
            // lccStatDstPath = /hbase/lcc/AAA/.lccindex/qualifier/BBB-stat
            String name = fileStatus.getPath().getName();
            Path lccStatDstPath =
                new Path(new Path(store.lccLocalHome, name.substring(0, name.length()
                    - LCCIndexConstant.LC_STAT_FILE_SUFFIX.length())), compactedFile.getName()
                    + LCCIndexConstant.LC_STAT_FILE_SUFFIX);
            if (!store.localfs.exists(lccStatDstPath.getParent())) {
              HBaseFileSystem.makeDirOnFileSystem(store.localfs, lccStatDstPath.getParent());
            }
            LOG.info("Renaming compacted local index stat file at " + fileStatus.getPath() + " to "
                + lccStatDstPath);
            if (!HBaseFileSystem.renameDirForFileSystem(store.localfs, fileStatus.getPath(),
              lccStatDstPath)) {
              WinterOptimizer.ThrowWhenCalled("Failed move of compacted index stat file "
                  + fileStatus.getPath() + " to " + lccStatDstPath);
            }
          } else {
            // fileStatus = /lcc/AAA/.tmp/.lccindex/qualifier
            // lccIndexDir = /hbase/lcc/AAA/f/.lccindex/qualifier/BBB
            Path lccIndexDstPath =
                new Path(new Path(store.lccLocalHome, fileStatus.getPath().getName()),
                    compactedFile.getName());
            // System.out.println("winter checking lccIndexDstPath: " + lccIndexDstPath);
            if (!store.localfs.exists(lccIndexDstPath.getParent())) {
              HBaseFileSystem.makeDirOnFileSystem(store.localfs, lccIndexDstPath.getParent());
            }
            if (printForDebug) {
              System.out.println("winter renaming compacted local index file at "
                  + fileStatus.getPath() + " to " + lccIndexDstPath);
            }
            LOG.info("Renaming compacted local index file at " + fileStatus.getPath() + " to "
                + lccIndexDstPath);
            if (!HBaseFileSystem.renameDirForFileSystem(store.localfs, fileStatus.getPath(),
              lccIndexDstPath)) {
              WinterOptimizer.NeedImplementation("Failed move of compacted index file "
                  + fileStatus.getPath() + " to " + lccIndexDstPath);
            }
          }
        }
        // delete if necessary
        fileStatusArray = store.localfs.listStatus(lccLocalTmpPath);
        if (fileStatusArray != null && fileStatusArray.length == 0) {
          store.localfs.delete(lccLocalTmpPath, true);
        } else if (fileStatusArray != null) {
          WinterOptimizer
              .ThrowWhenCalled("winter completeCompaction lcc dir should be empty but not: "
                  + lccLocalTmpPath + ", now size: " + fileStatusArray.length);
        }
      }
    }
  }
}
