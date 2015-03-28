package doWork.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import doWork.jobs.BasicJob.JobStatus;

public abstract class BasicJobQueue {
  static final Log LOG = LogFactory.getLog(BasicJobQueue.class);

  boolean started = false;
  protected List<BasicJob> jobs;
  protected ProcessRunnable theRun = null;
  protected Thread theThread = null;
  protected static long SLEEP_TIME = 100;
  protected final Object syncJobsObj = new Object();;

  public void startThread() {
    if (!started) {
      started = true;
      jobs = new ArrayList<BasicJob>();
      theRun = new ProcessRunnable(jobs, this.getClass().getName(), syncJobsObj);
      theThread = new Thread(theRun);
      theThread.start();
    }
  }

  public void stopAndJoin() throws InterruptedException {
    theRun.setToStop();
    theThread.join();
  }

  public void addJob(BasicJob job) {
    synchronized (syncJobsObj) {
      jobs.add(job);
    }
  }

  // if not processNow, promote the current job to the head and promotes related jobs
  public void promoteJob(BasicJob rawJob, boolean processNow) throws IOException {
    if (!checkJobClass(rawJob)) {
      throw new IOException("winter job queue " + this.getClass().getName() + " meet job: "
          + rawJob.getClass().getName());
    }
    BasicJob job = findJobInQueue(rawJob);
    if (job == null) {
      job = rawJob;
    }
    if (job.getStatus() == JobStatus.WAITING) {
      // 1. related job is waiting
      if (processNow) {
        if (job.checkReady()) { // process if ready
          job.work();
        } else { // promote related job and then work
          job.promoteRelatedJobs(processNow);
          if (!job.checkReady()) {
            throw new IOException("winter job still not ready after upgrade related job: "
                + job.getClass().getName());
          }
          job.work();
        }
      } else {
        putJobToQueueHead(job);
      }
    } else if (processNow) { // 2. related job is running and need to wait
      while (job.getStatus() != JobStatus.FINISHED) {
        try {
          Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } // 3. related job is running and not need to wait, just return
  }

  public void putJobToQueueHead(BasicJob job) throws IOException {
    synchronized (syncJobsObj) {
      if (jobs.contains(job)) {
        jobs.remove(job);
      }
      jobs.add(0, job);
    }
  }

  /**
   * @param job
   * @return the corresponding job, if waiting, remove the job from queue
   * @throws IOException
   */
  private BasicJob findJobInQueue(BasicJob job) throws IOException {
    synchronized (syncJobsObj) {
      for (BasicJob j : jobs) {
        if (job.correspondsTo(j)) {
          if (j.getStatus() == JobStatus.WAITING) {
            jobs.remove(j);
          }
          return j;
        }
      }
    }
    return null;
  }

  public abstract boolean checkJobClass(BasicJob job);

  public double getSleepFactor() {
    return 0;
    // int length = jobs.size();
    // if (length > 10) length = 10;
    // return 0.01 * length;
  }
}
