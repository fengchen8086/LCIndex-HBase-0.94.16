package doWork.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.IndexColumnDescriptor;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactSelection;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;

import doTestAid.WinterOptimizer;
import doWork.LCCIndexConstant;
import doWork.LCCIndexGenerator;
import doWork.file.LCIndexWriter;
import doWork.jobs.CommitJobQueue.CommitJob;
import doWork.jobs.CompleteCompactionJobQueue.CompleteCompactionJob;
import doWork.jobs.RemoteJobQueue.RemoteJob;

public class CompactJobQueue extends BasicJobQueue {

  private static final CompactJobQueue singleton = new CompactJobQueue();

  public static CompactJobQueue getInstance() {
    return singleton;
  }

  @Override
  public boolean checkJobClass(BasicJob job) {
    return job instanceof CompactJob;
  }

  public CompactJob findJobByDestPath(Path destPath) {
    synchronized (syncJobsObj) {
      for (BasicJob job : jobs) {
        CompactJob fJob = (CompactJob) job;
        if (destPath.compareTo(fJob.tmpHDFSPath) == 0) {
          return fJob;
        }
      }
    }
    return null;
  }

  public static class RebuildCompactJob extends CompactJob {

    protected Path majorMovedPath = null; // solid path
    protected Reader reader = null;

    public RebuildCompactJob(Store store, CompactionRequest request, Path writtenPath)
        throws IOException {
      super(store, request, writtenPath);
      if (printForDebug) {
        System.out.println("winter RebuildCompactJob construction, hdfsPath: " + tmpHDFSPath);
      }
    }

    public void setMajorReady(Path destPath, Reader reader) {
      this.majorMovedPath = destPath;
      this.reader = reader;
    }

    @Override
    public boolean checkReady() {
      return majorMovedPath != null && reader != null;
    }

    @Override
    public void work() throws IOException {
      doMajorCompact();
    }

    @Override
    protected void printRelatedJob() {
      System.out.println("winter major compact job has no related job, want " + tmpHDFSPath
          + ", now majorMovedPath is: " + majorMovedPath + ", reader is null: " + (reader == null));
    }

    private void doMajorCompact() throws IOException {
      // major, then read things from HDFS and merge them locally ...
      // however, local data must be deleted as well, but they may be handle in
      // Store.completeCompaction();
      // hdfs lccindex data also must be deleted as well
      StoreFileScanner compactedFileScanner = reader.getStoreFileScanner(false, false);
      IndexColumnDescriptor indexFamily = new IndexColumnDescriptor(store.getFamily());
      assert indexFamily.hasIndex()
          && indexFamily.getIndexType() == IndexColumnDescriptor.IndexType.LCCIndex;
      TreeMap<byte[], DataType> lccIndexQualifierType = indexFamily.mWinterGetQualifierType();
      String desStr =
          store.getHRegion().getTableDesc().getValue(LCCIndexConstant.LC_TABLE_DESC_RANGE_STR);
      LCCIndexGenerator lccindexGenerator = new LCCIndexGenerator(lccIndexQualifierType, desStr);
      KeyValue kv = null;
      List<Path> flushedIndexFiles = new ArrayList<Path>();
      LCIndexWriter lcIdxWriter = null;
      compactedFileScanner.seek(KeyValue.createFirstOnRow(HConstants.EMPTY_BYTE_ARRAY));
      while ((kv = compactedFileScanner.next()) != null) {
        // winter should consider cache size, but not now
        // winter see store.closeCheckInterval, important but optimize it later
        lccindexGenerator.processKeyValue(kv);
      }
      // flush new!
      KeyValue[] lccIndexResultArray = lccindexGenerator.generatedSortedKeyValueArray();
      if (lccIndexResultArray != null && lccIndexResultArray.length > 0) {
        if (printForDebug) {
          System.out.println("winter major compact want file: " + tmpHDFSPath);
        }
        lcIdxWriter =
            new LCIndexWriter(store, tmpHDFSPath, lccindexGenerator.getResultStatistic(),
                lccindexGenerator.getLCRangeStatistic(), null);
        for (KeyValue lcckv : lccIndexResultArray) {
          lcIdxWriter.append(lcckv);
        }
      } else {
        System.out.println("winter CompactLCCIndex the lccResults is zero, how come?!");
      }
      if (lcIdxWriter != null) {
        lcIdxWriter.close(true);
        flushedIndexFiles.add(lcIdxWriter.getBaseDirPath());
      }
      lccindexGenerator.clear();
      if (compactedFileScanner != null) compactedFileScanner.close();
      if (flushedIndexFiles.size() == 1) {
        // Path desPath = store.getLocalBaseDirByFileName(hfilePath);
      } else {
        WinterOptimizer.ThrowWhenCalled("winter CompactLCCIndex see flushedIndexFiles.size() = "
            + flushedIndexFiles.size());
      }
    }

    @Override
    public void promoteRelatedJobs(boolean processNow) throws IOException {
      System.out.println("RebuildCompact job have noJob to promote");
    }
  }

  public static class NormalCompactJob extends CompactJob {
    protected List<CommitJob> relatedJobs;
    protected TreeMap<Path, List<Path>> allIdxPaths;
    protected TreeMap<Path, BasicJob> missPathJob;

    public NormalCompactJob(Store store, CompactionRequest request, Path writtenPath)
        throws IOException {
      super(store, request, writtenPath);
      if (printForDebug) {
        System.out.println("winter NormalCompactJob construction, hdfsPath: " + tmpHDFSPath);
      }
      relatedJobs = new ArrayList<CommitJob>();
      allIdxPaths = new TreeMap<Path, List<Path>>();
      missPathJob = new TreeMap<Path, BasicJob>();
      destLCIndexDir = store.getLocalBaseDirByFileName(writtenPath);
      List<Path> lcQualifierDirs = getLCQualifierDirs();
      for (Path lcQualifierDir : lcQualifierDirs) {
        // lcQualifierDir = /hbase/lcc/AAA/f/.lccindex/Q1-...-Q4
        List<Path> idxPaths = new ArrayList<Path>();
        for (StoreFile rawCompactedFile : request.getFiles()) {
          // rawCompactedFile = lcc/AAA/f/BBB, AAA is always the same
          // lcIdxFile = lcc/AAA/f/.lccindex/qualifier/BBB. In this loop, Bi is changing
          Path lcIdxFile = new Path(lcQualifierDir, rawCompactedFile.getPath().getName());
          idxPaths.add(lcIdxFile);
          if (!store.localfs.exists(lcIdxFile)) {
            if (printForDebug) {
              System.out.println("winter finding the lcIdxFile to be compacted: " + lcIdxFile);
            }
            CommitJob commitJob =
                CommitJobQueue.getInstance().findJobByDestPath(rawCompactedFile.getPath());
            if (commitJob == null) {
              // not necessary to process now, just wait
              CompleteCompactionJob completeJob =
                  CompleteCompactionJobQueue.getInstance().findJobByDestPath(
                    rawCompactedFile.getPath());
              if (completeJob == null) {
                System.out
                    .println("winter file not found on locally, try to find it on remote servers: "
                        + lcIdxFile + ", corresponding raw path: " + rawCompactedFile.getPath());
                RemoteJob job = new RemoteJob(store, lcIdxFile, rawCompactedFile.getPath(), false);
                RemoteJobQueue.getInstance().addJob(job);
                missPathJob.put(lcIdxFile, job);
              } else {
                System.out.println("winter CompactJob found file in complete job queue");
                missPathJob.put(lcIdxFile, completeJob);
              }
            } else {
              System.out.println("winter CompactJob found file in commit job queue");
              missPathJob.put(lcIdxFile, commitJob);
            }
          }
        }
        allIdxPaths.put(lcQualifierDir, idxPaths);
      }
    }

    @Override
    public boolean checkReady() {
      // if all related jobs are finished, then report ready
      List<Path> toDelete = new ArrayList<Path>();
      for (Entry<Path, BasicJob> entry : missPathJob.entrySet()) {
        if (entry.getValue().getStatus() == JobStatus.FINISHED) {
          toDelete.add(entry.getKey());
        }
      }
      for (Path p : toDelete) {
        missPathJob.remove(p);
      }
      return missPathJob.size() == 0;
    }

    private List<Path> getLCQualifierDirs() throws IOException {
      // store.mWinterGetLCCIndexFilePathFromHFilePathInTmp(hfilePath);
      if (!store.localfs.exists(destLCIndexDir)) {
        System.out.println("winter target minor compact dir not exists, create: " + destLCIndexDir);
        if (!store.localfs.mkdirs(destLCIndexDir)) {
          WinterOptimizer.ThrowWhenCalled("winter error in mkdir: " + destLCIndexDir);
        }
      }
      // lccLocalHome/lcc/AAA/f/.lccindex
      FileStatus[] fileStatusList = store.localfs.listStatus(store.lccLocalHome);
      ArrayList<Path> lcDirs = new ArrayList<Path>();
      if (fileStatusList == null || fileStatusList.length == 0) {
        // need to copy from remote!
        for (byte[] qualifierName : store.lccIndexQualifierType.keySet()) {
          Path remotePath = new Path(store.lccLocalHome, Bytes.toString(qualifierName));
          lcDirs.add(remotePath);
        }
      } else {
        for (FileStatus fileStatus : fileStatusList) {
          if (fileStatus.getPath().getName().endsWith(LCCIndexConstant.LC_STAT_FILE_SUFFIX)) {
            continue;
          }
          lcDirs.add(fileStatus.getPath());
        }
      }
      return lcDirs;
    }

    @Override
    public void promoteRelatedJobs(boolean processNow) throws IOException {
      List<BasicJob> promotedJobs = new ArrayList<BasicJob>();
      for (Entry<Path, BasicJob> entry : missPathJob.entrySet()) {
        if (promotedJobs.contains(entry.getValue())) {
          continue;
        } else {
          promotedJobs.add(entry.getValue());
          // commit or remote
          entry.getValue().promoteRelatedJobs(processNow);
        }
      }
    }

    @Override
    protected void printRelatedJob() {
      System.out.println("winter minor compact hdfsPath is: " + tmpHDFSPath
          + ", list waiting jobs: ");
      for (Entry<Path, BasicJob> entry : missPathJob.entrySet()) {
        System.out.println("winter minor compact job for path: " + entry.getKey()
            + " waiting for another job: " + entry.getValue().getStatus()
            + ", relatedFlushJob id: " + entry.getValue().id + ", this id: " + id);
        entry.getValue().printRelatedJob();
      }
    }

    private void doMinorCompact() throws IOException {
      // hfilePath = lcc/AAA/.tmp/BBB/
      // destLCIndexDir = lcc/AAA/.lctmp/BBB.lccindex
      // every qualifier must be merged seperately!
      // merge B1-Q1, B2-Q1, B3-Q1 into B-target-Q1
      // shoud not use
      for (Entry<Path, List<Path>> entry : allIdxPaths.entrySet()) {
        List<StoreFile> lccSFList = new ArrayList<StoreFile>();
        List<Path> statPathList = new ArrayList<Path>();
        for (Path lcIdxPath : entry.getValue()) {
          StoreFile lccSF =
              new StoreFile(store.localfs, lcIdxPath, store.conf, store.cacheConf, store
                  .getFamily().getBloomFilterType(), store.getDataBlockEncoder());
          lccSF.createReader();
          lccSFList.add(lccSF);
          statPathList.add(new Path(lcIdxPath.getParent(), lcIdxPath.getName()
              + LCCIndexConstant.LC_STAT_FILE_SUFFIX));
        }
        CompactSelection lccIndexFilesToCompactCS = new CompactSelection(store.conf, lccSFList);
        CompactionRequest lccCR =
            new CompactionRequest(request.getHRegion(), store, lccIndexFilesToCompactCS,
                request.isMajor(), request.getPriority());
        long maxId = StoreFile.getMaxSequenceIdInList(lccSFList, true);
        Path destPath = new Path(destLCIndexDir, entry.getKey().getName());
        // compact stat file, and then compact storefile
        store.compactor.mWinterCompactStatFile(store.localfs, statPathList, new Path(
            destLCIndexDir, entry.getKey().getName() + LCCIndexConstant.LC_STAT_FILE_SUFFIX));
        StoreFile.Writer writer = store.compactor.lcIdxCompact(lccCR, maxId, destPath, true);
        for (StoreFile sf : lccSFList) {
          sf.closeReader(true);
        }
        if (printForDebug) {
          System.out.println("winter minor compact flush to: " + writer.getPath());
        }
      }
    }

    @Override
    public void work() throws IOException {
      doMinorCompact();
    }
  }

  public static abstract class CompactJob extends BasicJob {
    protected Store store;
    protected CompactionRequest request;
    protected Path tmpHDFSPath; // tmp Path
    protected Path destLCIndexDir; // tmp Path

    public CompactJob(Store store, final CompactionRequest request, final Path writtenPath)
        throws IOException {
      this.store = store;
      this.request = request;
      this.tmpHDFSPath = writtenPath;
    }

    @Override
    public boolean correspondsTo(BasicJob j) throws IOException {
      return this.tmpHDFSPath.equals(((CompactJob) j).tmpHDFSPath);
    }
  }
}
