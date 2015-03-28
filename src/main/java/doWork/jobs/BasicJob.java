package doWork.jobs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public abstract class BasicJob {

  enum JobStatus {
    WAITING, DOING, FINISHED
  }

  protected static AtomicLong idCounter = new AtomicLong(0);
  protected boolean printForDebug = true;
  protected long id = idCounter.getAndIncrement();
  protected JobStatus status = JobStatus.WAITING;
  protected long startTime = System.currentTimeMillis();
  protected long testCount = 0;
  protected long showInterval = 1000;

  public abstract boolean checkReady();

  public boolean isReady() {
    ++testCount;
    if (checkReady()) {
      System.out.println("winter job: " + this.getClass().getName() + " ready after " + testCount
          + " times");
      return true;
    } else if (testCount % showInterval == 0) {
      System.out.println("winter job: " + this.getClass().getName() + " still not ready after "
          + testCount + " times try");
      printRelatedJob();
    }
    return false;
  }

  protected abstract void printRelatedJob();

  public abstract boolean correspondsTo(BasicJob j) throws IOException;

  public abstract void promoteRelatedJobs(boolean processNow) throws IOException;

  public abstract void work() throws IOException;

  public JobStatus getStatus() {
    return status;
  }

  public void setStatus(JobStatus status) {
    this.status = status;
  }

}
