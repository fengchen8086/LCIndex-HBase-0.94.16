package doWork.jobs;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseFileSystem;
import org.apache.hadoop.hbase.regionserver.Store;

import doTestAid.WinterOptimizer;
import doWork.LCCIndexConstant;
import doWork.jobs.FlushJobQueue.FlushJob;

public class CommitJobQueue extends BasicJobQueue {

  private static final CommitJobQueue singleton = new CommitJobQueue();

  public static CommitJobQueue getInstance() {
    return singleton;
  }

  @Override
  public boolean checkJobClass(BasicJob job) {
    return job instanceof CommitJob;
  }

  public CommitJob findJobByDestPath(Path destPath) {
    synchronized (syncJobsObj) {
      for (BasicJob job : jobs) {
        CommitJob fJob = (CommitJob) job;
        if (destPath.compareTo(fJob.destPath) == 0) {
          return fJob;
        }
      }
    }
    return null;
  }

  public static class CommitJob extends BasicJob {
    protected FlushJob relatedFlushJob = null;
    protected Store store;
    protected Path rawPath; // tmp Path
    protected Path destPath; // solid Path

    public CommitJob(Store store, Path path, Path dest) {
      this.store = store;
      this.rawPath = path;
      this.destPath = dest;
      relatedFlushJob = FlushJobQueue.getInstance().findJobByRawPath(rawPath);
      if (printForDebug) {
        System.out.println("winter CommitJob construction, rawPath: " + rawPath + ", dest path: "
            + destPath);
      }
    }

    @Override
    public boolean checkReady() {
      return relatedFlushJob == null || relatedFlushJob.getStatus() == JobStatus.FINISHED;
    }

    @Override
    protected void printRelatedJob() {
      if (relatedFlushJob == null) {
        System.out.println("winter CommitJob.relatedFlushJob is null, just waiting ");
      } else {
        System.out.println("winter CommitJob is waiting for relatedFlushJob: "
            + relatedFlushJob.getStatus() + ", relatedFlushJob id: " + relatedFlushJob.id
            + ", this id: " + id);
        relatedFlushJob.printRelatedJob();
      }
    }

    @Override
    public boolean correspondsTo(BasicJob j) throws IOException {
      // CommitJob only care about the destPath
      return this.destPath.equals(((CommitJob) j).destPath);
    }

    @Override
    public void promoteRelatedJobs(boolean processNow) throws IOException {
      if (relatedFlushJob != null) {
        if (processNow) {
          if (relatedFlushJob.checkReady()) {
            relatedFlushJob.work();
          }
        } else {
          FlushJobQueue.getInstance().putJobToQueueHead(relatedFlushJob);
        }
      }
    }

    @Override
    public void work() throws IOException {
      // file: lcc/.lctmp/d6c3f4e4ad5149aa8f98ce2bae815460.lccindex/priority
      // to
      // lcc/207525d39bf02b403759fbc13af3c0ab/f/.lccindex/priority/d6c3f4e4ad5149aa8f98ce2bae815460
      mWinterCommitLCCLocal(rawPath);
    }

    private void mWinterCommitLCCLocal(final Path path) throws IOException {
      // target must be f/.lccindex/Q1-Q4/BBB
      // path = /hbase/lcc/AAA/.tmp/BBB
      Path lccLocalTmpPath = store.getLocalBaseDirByFileName(path);
      // before commit, dir lccLocalPath have already got files from flush or commit
      FileStatus[] lccLocalTempFileStatus = store.localfs.listStatus(lccLocalTmpPath);
      if (lccLocalTempFileStatus != null && lccLocalTempFileStatus.length > 0) {
        for (FileStatus fileStatus : lccLocalTempFileStatus) {
          if (fileStatus.getPath().getName().endsWith(LCCIndexConstant.LC_STAT_FILE_SUFFIX)) continue;
          // fileStatus = Local_Home/lcc/AAA/.tmp/BBB.lccindex/Q1-Q4
          // indexQualifierPath = Local_Home/hbase/lcc/AAA/f/.lccindex/Q1-Q4
          // lccLocalHome = Local_Home/lcc/AAA/f/.lccindex
          Path indexQualifierPath = new Path(store.lccLocalHome, fileStatus.getPath().getName());
          // create dir if not existed
          if (!store.localfs.exists(indexQualifierPath)) {
            HBaseFileSystem.makeDirOnFileSystem(store.localfs, indexQualifierPath);
          }
          // move stat file
          Path rawStatFile =
              new Path(fileStatus.getPath().getParent(), fileStatus.getPath().getName()
                  + LCCIndexConstant.LC_STAT_FILE_SUFFIX);
          Path destStatFile =
              new Path(indexQualifierPath, path.getName() + LCCIndexConstant.LC_STAT_FILE_SUFFIX);
          // System.out.println("winter move raw stat file: " + rawStatFile + " to " +
          // targetStatFile);
          if (!HBaseFileSystem.renameDirForFileSystem(store.localfs, rawStatFile, destStatFile)) {
            LOG.warn("Unable to rename lccindex local stat file " + rawStatFile + " to "
                + destStatFile);
          }
          // move index file
          // lccIndexDestPath = HOME/lcc/AAA/f/.lccindex/Q1-Q4/BBB
          Path lccIndexDestPath = new Path(indexQualifierPath, path.getName());
          if (printForDebug) {
            System.out.println("winter renaming flushed lccindex local file from "
                + fileStatus.getPath() + " to " + lccIndexDestPath);
          }
          if (!HBaseFileSystem.renameDirForFileSystem(store.localfs, fileStatus.getPath(),
            lccIndexDestPath)) {
            LOG.warn("Unable to rename lccindex local file " + fileStatus.getPath() + " to "
                + lccIndexDestPath);
          }
        }
        // delete dir if necessary
        lccLocalTempFileStatus = store.localfs.listStatus(lccLocalTmpPath);
        if (lccLocalTempFileStatus != null && lccLocalTempFileStatus.length == 0) {
          store.localfs.delete(lccLocalTmpPath, true);
        } else if (lccLocalTempFileStatus != null) {
          WinterOptimizer.ThrowWhenCalled("winter commit local lcc dir should be empty but not: "
              + lccLocalTmpPath);
        }
      } else {
        // no lccindex exists
        System.out.println("winter lccIndexPath local dir not exist: " + lccLocalTmpPath);
      }
    }
  }
}
