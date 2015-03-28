package doWork.jobs;

import java.io.IOException;
import java.util.List;

import doWork.jobs.BasicJob.JobStatus;

public class ProcessRunnable implements Runnable {

  enum QueueStatus {
    EMPTY, NO_ONE_READY, DOING_ONE;
  }

  private final String jobName;
  private List<BasicJob> jobs;
  private boolean keepRun = true;
  private static final long SLEEP_TIME = 100;
  private final Object syncJobsObj;

  public ProcessRunnable(List<BasicJob> jobs, String jobName, Object obj) {
    this.jobs = jobs;
    this.jobName = jobName;
    this.syncJobsObj = obj;
  }

  public void setToStop() {
    keepRun = false;
  }

  @Override
  public void run() {
    while (keepRun) {
      try {
        QueueStatus ret = innerWork();
        if (ret == QueueStatus.EMPTY) {
          Thread.sleep(SLEEP_TIME);
        } else if (ret == QueueStatus.NO_ONE_READY) {
          Thread.sleep(SLEEP_TIME / 2);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    try {
      while (innerWork() != QueueStatus.EMPTY) {
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private BasicJob findReadyJob() {
    synchronized (syncJobsObj) {
      for (BasicJob job : jobs) {
        if (job.isReady()) {
          job.setStatus(JobStatus.DOING);
          return job;
        }
      }
    }
    return null;
  }

  int queueCounter = 0; // all queues!
  static int QUEUE_REPORT_INTERVAL = 2000; // for all queues!
  static int EMPTY_PRINT_INTERVAL = 100;
  int emptyQueueCounter = 0;
  int reachedMaxSize = 0;
  int noReadyCheckCount = 0;
  int noReadyMaxRound = 100;

  // return true if empty
  private QueueStatus innerWork() throws InterruptedException, IOException {
    QueueStatus ret = QueueStatus.NO_ONE_READY;
    int size = 0;
    synchronized (syncJobsObj) {
      size = jobs.size();
    }
    if (reachedMaxSize < size) {
      System.out.println("winter job class: " + jobName + " readching new maxSize: " + size);
      reachedMaxSize = size;
    }
    if (size == 0) {
      noReadyCheckCount = 0;
      if (++emptyQueueCounter == EMPTY_PRINT_INTERVAL) {
        emptyQueueCounter = 0;
//        System.out.println("winter job class: " + jobName + " empty for: " + EMPTY_PRINT_INTERVAL
//            + " times");
      }
      return QueueStatus.EMPTY;
    }
    BasicJob job = findReadyJob();
    if (job != null) { // one job is ready to run
      noReadyCheckCount = 0;
      long time = System.currentTimeMillis() - job.startTime;
//      System.out.println("winter job " + job.getClass().getName() + " cost " + time / 1000.0
//          + " seconds before start working");
      try {
        job.work();
      } catch (IOException e) {
        System.out.println("**********winter IOException happends on job "
            + job.getClass().getName() + ", id: " + job.id);
        job.printRelatedJob();
        e.printStackTrace();
        System.out.println("**********winter IOException print done on job "
            + job.getClass().getName() + ", id: " + job.id);
      } finally {
        time = System.currentTimeMillis() - job.startTime;
//        System.out.println("winter job " + job.getClass().getName() + " cost " + time / 1000.0
//            + " seconds before finished");
        job.setStatus(JobStatus.FINISHED);
        synchronized (syncJobsObj) {
          jobs.remove(job);
        }
        ret = QueueStatus.DOING_ONE;
      }
    } else {
      if (++noReadyCheckCount > noReadyMaxRound) {
//        System.out.println("winter job class: " + jobName + " no one ready for "
//            + noReadyCheckCount + " times");
      }
    }
    if (++queueCounter == QUEUE_REPORT_INTERVAL) {
      queueCounter = 0;
//      System.out.println("winter job class: " + jobName + " with queue size: " + size);
    }
    return ret;
  }
}
