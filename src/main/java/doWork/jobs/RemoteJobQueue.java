package doWork.jobs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.Store;

import doWork.file.LCCHFileMoverClient;
import doWork.file.LCCHFileMoverClient.RemoteStatus;

public class RemoteJobQueue extends BasicJobQueue {

  private static final RemoteJobQueue singleton = new RemoteJobQueue();

  public static RemoteJobQueue getInstance() {
    return singleton;
  }

  @Override
  public boolean checkJobClass(BasicJob job) {
    return job instanceof RemoteJob;
  }

  public static class RemoteJob extends BasicJob {
    private Store store;
    private Path realPath;
    private Path hdfsPath;
    private boolean isDelete;
    private RemoteStatus lastTime;

    /**
     * @param store
     * @param path
     * @param isDelete true for delete, false for get
     */
    public RemoteJob(Store store, Path path, Path hdfsPath, boolean isDelete) {
      this.store = store;
      this.realPath = path;
      this.hdfsPath = hdfsPath;
      this.isDelete = isDelete;
      this.lastTime = RemoteStatus.UNKNOWN;
      if (printForDebug) {
        System.out.println("winter RemoteJob construction, read file: " + realPath + ", hdfsPath: "
            + hdfsPath + ", isDelete: " + isDelete);
      }
    }

    @Override
    public boolean checkReady() {
      // try to get the file remotely! if all remote jobs are finished, then return yes
      return true;
    }

    @Override
    protected void printRelatedJob() {
      System.out.println("winter RemoteJob want file: " + realPath + ", hdfsPath: " + hdfsPath
          + ", isDelete: " + isDelete + ", this id " + id);
    }

    @Override
    public boolean correspondsTo(BasicJob j) throws IOException {
      return this.hdfsPath.equals(((RemoteJob) j).hdfsPath);
    }

    @Override
    public void promoteRelatedJobs(boolean processNow) throws IOException {
      // this need to operator remotely
      if (lastTime != RemoteStatus.SUCCESS) {
        lastTime = operateRemoteLCLocalFile(store, realPath, hdfsPath, processNow, isDelete);
        if (lastTime != RemoteStatus.SUCCESS && lastTime != RemoteStatus.IN_QUEUE_WAITING) {
          throw new IOException("winter remote job fail for file: " + realPath + ", hdfsPath: "
              + hdfsPath + ", RemoteStatus: " + lastTime);
        }
      }
    }

    @Override
    public void work() throws IOException {
      if (lastTime != RemoteStatus.SUCCESS) {
        RemoteStatus ret = operateRemoteLCLocalFile(store, realPath, hdfsPath, true, isDelete);
        if (ret != RemoteStatus.SUCCESS) {
          throw new IOException("winter remote job fail for file: " + realPath + ", hdfsPath: "
              + hdfsPath + ", RemoteStatus: " + ret + ", isDelete: " + isDelete);
        }
      }
    }

    public static RemoteStatus operateRemoteLCLocalFile(Store store, Path realPath, Path hdfsPath,
        boolean processNow, boolean isDelete) throws IOException {
      LCCHFileMoverClient moverClient;
      if (store.lccPrevHoldServer != null) {
        moverClient = new LCCHFileMoverClient(store.lccPrevHoldServer, store.conf);
        RemoteStatus ret =
            isDelete ? moverClient.deleteRemoteFile(parsePath(realPath)) : moverClient.copyRemoteFile(
              parsePath(realPath), hdfsPath, processNow);
        if (ret == RemoteStatus.SUCCESS || ret == RemoteStatus.IN_QUEUE_WAITING) {
          System.out.println("winter succeed in " + (isDelete ? "deleting " : "finding ") + realPath
              + " on " + store.lccPrevHoldServer + ", status: " + ret);
          return ret;
        }
      }
      for (String hostname : store.lccRegionServerHostnames) {
        if (hostname.equals(store.lccPrevHoldServer)) {
          continue;
        }
        moverClient = new LCCHFileMoverClient(hostname, store.conf);
        RemoteStatus ret =
            isDelete ? moverClient.deleteRemoteFile(parsePath(realPath)) : moverClient.copyRemoteFile(
              parsePath(realPath), hdfsPath, processNow);
        if (ret == RemoteStatus.SUCCESS || ret == RemoteStatus.IN_QUEUE_WAITING) {
          System.out.println("winter succeed in " + (isDelete ? "deleting " : "finding ") + realPath
              + " on " + hostname + ", status: " + ret);
          return ret;
        }
      }
      if (processNow) {
        System.out.println("winter failed in " + (isDelete ? "deleting " : "finding ") + realPath
            + " on all hosts");
      }
      store.lccPrevHoldServer = null;
      return RemoteStatus.NOT_EXIST;
    }

    public static String parsePath(Path p) {
      // p = file://xxxx/xxx/xxxx, trans to /xxxx/xxx/xxxx
      int depth = p.depth();
      String str = "";
      while (depth > 0) {
        str = Path.SEPARATOR + p.getName() + str;
        p = p.getParent();
        --depth;
      }
      return str;
    }

  }

}
