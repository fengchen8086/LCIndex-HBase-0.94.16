package org.apache.hadoop.hbase.ccindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ccindex.IndexSpecification.IndexType;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Scanner which you can read query results from.
 * @author wanhao
 */
public class IndexResultScanner {
  private static final Log LOG = LogFactory.getLog(IndexResultScanner.class);

  private byte[][] resultColumns;

  // each SingleScanner process a ResultScanner
  private List<SingleScanner> scanners;

  // result buffer
  private LinkedBlockingQueue<Result> resultBuffer;

  private int resultBufferSize = 0;

  // when buffer has less than resultBufferSize*loadFactor results,
  // start the interrupted scan threads if scan is not ended
  private float loadFactor = 0.75f;

  // resultBufferSize*loadFactor
  private int minLoadSize = 0;

  // total SingleScanner
  private int scannerNum = 0;

  // list of scanners which have finished scanning
  private ArrayList<String> finishedScanner;
  private Object finishedLock = new Object();

  // list of scanners which have not finished but stopping scanning because of exceptions
  private ArrayList<String> stoppedScanner;
  private Object stoppedLock = new Object();

  // total result records
  private long recordCounter = 0;

  private long startTime = 0;
  private long totalTime = 0;

  protected int restartTimes;
  protected int MAX_RESTART_TIMES;

  private Result result;

  public IndexResultScanner(List<SingleScanner> scanners, byte[][] resultColumns,
      int resultBufferSize, float loadFactor) {
    this.scanners = scanners;
    this.resultColumns = resultColumns;
    this.resultBufferSize = resultBufferSize;
    this.loadFactor = loadFactor;
    this.minLoadSize = (int) (this.resultBufferSize * this.loadFactor);
    this.resultBuffer = new LinkedBlockingQueue<Result>(resultBufferSize);

    LOG.debug("IndexResultScanner is started!");

    this.scannerNum = this.scanners.size();
    this.finishedScanner = new ArrayList<String>();
    this.stoppedScanner = new ArrayList<String>();

    this.startTime = System.currentTimeMillis();

    int i = 0;
    for (SingleScanner scanner : this.scanners) {
      scanner.setName("Scanner" + i++);
      scanner.setIndexResultScanner(this);
      scanner.start();
    }
    this.restartTimes = 0;
    this.MAX_RESTART_TIMES = HBaseConfiguration.create().getInt("hbase.client.retries.number", 10);
  }

  /**
   * Get the next result.
   * <p>
   * If current result buffer size is less than MaxSize*LoadFactor and there are stopped scanners,
   * the stopped scanners will be restart automatically.
   * @return if there is any result in the {@link #resultBuffer}, return one, otherwise return null.
   * @throws InterruptedException
   */
  public Result next() {
    if (this.resultBuffer.size() < this.minLoadSize) {
      if (stoppedScanner.size() != 0) {
        synchronized (stoppedLock) {
          if (stoppedScanner.size() != 0) {
            if (restartTimes < MAX_RESTART_TIMES) {
              restartTimes++;
              for (int i = 0; i < stoppedScanner.size(); i++) {
                int index = Integer.valueOf(stoppedScanner.remove(i));
                scanners.get(index).restartScan();
              }
            } else {
              for (int i = 0; i < stoppedScanner.size(); i++) {
                int index = Integer.valueOf(stoppedScanner.remove(i));
                scanners.get(index).close();
              }
            }
            stoppedLock.notifyAll();
          }
        }
      }
    }

    try {
      this.result = this.resultBuffer.poll(10, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn(e);
    }

    // If read successfully, just return
    if (this.result != null) {
      this.recordCounter++;
      return this.result;
    } else {
      while (true) {

        if (this.resultBuffer.size() < this.minLoadSize) {
          if (stoppedScanner.size() != 0) {
            synchronized (stoppedLock) {
              if (stoppedScanner.size() != 0) {
                if (restartTimes < MAX_RESTART_TIMES) {
                  restartTimes++;
                  for (int i = 0; i < stoppedScanner.size(); i++) {
                    int index = Integer.valueOf(stoppedScanner.remove(i));
                    scanners.get(index).restartScan();
                  }
                } else {
                  for (int i = 0; i < stoppedScanner.size(); i++) {
                    int index = Integer.valueOf(stoppedScanner.remove(i));
                    scanners.get(index).close();
                  }
                }
                stoppedLock.notifyAll();
              }
            }
          }
        }

        if (this.isAllScannerFinished()) {
          try {
            this.result = this.resultBuffer.poll(10, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            LOG.warn(e.getMessage());
          }
          if (this.result != null) {
            this.recordCounter++;
          }
          return this.result;
        } else {
          try {
            this.result = this.resultBuffer.poll(10, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            LOG.warn(e.getMessage());
          }
          if (this.result != null) {
            this.recordCounter++;
            return this.result;
          }
        }
      }

    }

  }

  public List<SingleScanner> getSingleScanners() {
    return this.scanners;
  }

  /**
   * One or more {@link SingleScanner} has finished scan.
   */
  private void addFinished(String scannerName) {
    synchronized (finishedLock) {
      String scannerID = scannerName.replace("Scanner", "");
      this.finishedScanner.add(scannerID);
      if (LOG.isDebugEnabled()) {
        SingleScanner s = scanners.get(Integer.valueOf(scannerID));
        LOG.debug(scannerName + "-" + s.getId() + " is added to finished scanner!");
      }

      if (this.finishedScanner.size() == this.scannerNum) {
        this.totalTime = System.currentTimeMillis() - this.startTime;
      }
    }
  }

  /**
   * One or more {@link SingleScanner} has stopped scan because of Exceptions.
   */
  private void addStopped(String scannerName) {
    synchronized (stoppedLock) {
      String scannerID = scannerName.replace("Scanner", "");
      this.stoppedScanner.add(scannerID);
      try {
        stoppedLock.wait();
      } catch (InterruptedException e) {

      }
    }
  }

  public int getStoppedScannerNum() {
    synchronized (stoppedLock) {
      return this.stoppedScanner.size();
    }
  }

  /**
   * If all the {@link SingleScanner} have finished scanning.
   * @return If all are finished, return true, else return false.
   */
  public boolean isAllScannerFinished() {
    synchronized (finishedLock) {
      return this.finishedScanner.size() == this.scannerNum;
    }
  }

  /**
   * Get the number of scanners which have finished scanning.
   * @return
   */
  public int getFinishedScannerNum() {
    synchronized (finishedLock) {
      return this.finishedScanner.size();
    }
  }

  public int getTotalScannerNum() {
    return this.scannerNum;
  }

  /**
   * Stop all the running {@link NoIndexSingleScanner}.
   */
  public void close() {
    boolean anyAlive = true;

    while (anyAlive) {
      anyAlive = false;
      for (SingleScanner temp : scanners) {
        if (temp.isAlive()) {
          LOG.debug(temp.getName() + "-" + temp.getId() + " is alive, try to close it!");
          anyAlive = true;
          temp.close();
        } else {
          LOG.debug(temp.getName() + "-" + temp.getId() + " is closed!");
        }
      }
      synchronized (stoppedLock) {
        stoppedLock.notifyAll();
      }

      if (anyAlive) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
        }
      }

    }

    this.resultBuffer.clear();
    this.resultBuffer = null;
    LOG.debug("IndexResultScanner is closed!");
  }

  /**
   * Get total scan time. If scan is already ended, return total elapsed time during scanning, else
   * return total elapsed time from starting scanning until current.
   * @return time
   */
  public long getTotalScanTime() {
    synchronized (finishedLock) {
      if (this.finishedScanner.size() == this.scannerNum) {
        return this.totalTime;
      } else {
        return System.currentTimeMillis() - this.startTime;
      }
    }
  }

  /**
   * Get the number of results which have already been took out from result buffer with next()
   * method and results that are still in result buffer.
   * @return records number
   */
  public long getTookOutCount() {
    return this.recordCounter;
  }

  /**
   * Get the total number of results, including results already be took out from result buffer with
   * next() method and results that are still in result buffer.
   * @return
   */
  public long getTotalCount() {
    long counter = 0;
    for (SingleScanner temp : this.scanners) {
      counter += temp.getCount();
    }
    return counter;
  }

  public byte[][] gerResultColumns() {
    return resultColumns;
  }

  /**
   * Reader to process a {@link ResultScanner}.
   * @author liujia, modified by wanhao
   */
  static abstract class SingleScanner extends Thread {
    protected final Log LOG = LogFactory.getLog(this.getClass());
    protected byte[][] resultColumns;
    protected HTable indexTable;
    protected Scan scan;
    protected byte[] currentStartKey;
    protected boolean scanning;
    protected boolean finished;
    protected ResultScanner resultScanner;
    protected IndexResultScanner indexResultScanner;
    protected LinkedBlockingQueue<Result> resultBuffer;
    protected int count = 0;

    public SingleScanner(Scan scan, byte[][] resultColumns, HTable table) throws IOException {
      this.scan = scan;
      this.resultColumns = resultColumns;
      this.indexTable = table;
      this.currentStartKey = scan.getStartRow();
      this.scanning = true;
      this.finished = false;
      scan.setCacheBlocks(false);
      this.resultScanner = table.getScanner(scan);
      LOG.debug("scan caching:" + scan.getCaching());
    }

    public void setIndexResultScanner(IndexResultScanner irs) {
      this.indexResultScanner = irs;
      this.resultBuffer = irs.resultBuffer;
    }

    public int getCount() {
      return this.count;
    }

    public boolean isFinished() {
      return this.finished;
    }

    public boolean isScanning() {
      return this.scanning;
    }

    public void restartScan() {
      if (finished == false && scanning == false) {
        LOG.debug("Try to restart " + this.getName() + "-" + getId() + " for the "
            + indexResultScanner.restartTimes + "th times");
        scan.setStartRow(Bytes.add(currentStartKey, IndexConstants.MIN_ROW_KEY));
        try {
          resultScanner = indexTable.getScanner(scan);
          scanning = true;
        } catch (IOException e) {
          LOG.error("Try to restart " + this.getName() + "-" + getId() + " failed!" + e.toString());
        }

      } else {
        LOG.warn(this.getName() + "-" + getId() + " restart is ignored, because finished="
            + finished + ",scanning=" + scanning);
      }
    }

    public void close() {
      scanning = false;
      finished = true;
    }
  }

  /**
   * Scanner for scanning main data table.
   * @author wanhao
   */
  static class NoIndexSingleScanner extends SingleScanner {

    public NoIndexSingleScanner(Scan scan, byte[][] resultColumns, HTable table) throws IOException {
      super(scan, resultColumns, table);
    }

    public void run() {
      LOG.debug(this.getName() + "-" + getId() + " start scanning, startkey:"
          + Bytes.toStringBinary(scan.getStartRow()) + ", endkey:"
          + Bytes.toStringBinary(scan.getStopRow()));

      Result rs;

      while (!finished) {
        try {
          while ((rs = resultScanner.next()) != null && scanning) {
            while (!resultBuffer.offer(rs, 10, TimeUnit.MILLISECONDS)) {
              if (!scanning) {
                break;
              }
            }
            currentStartKey = rs.getRow();
            count++;
          }
          if (scanning) {
            finished = true;
          }
        } catch (Exception e) {
          LOG.warn(this.getName() + "-" + getId() + " stop scanning!", e);
        } finally {
          resultScanner.close();
        }

        if (!finished) {
          scanning = false;
          LOG.debug(this.getName() + "-" + getId() + " stop scanning, waiting to be restarted!");
          indexResultScanner.addStopped(getName());
          LOG.debug(this.getName() + "-" + getId() + " is waked up, restart scan!");
        } else {
          indexResultScanner.addFinished(getName());
          LOG.debug(this.getName() + "-" + getId() + " finish scanning," + " total records:"
              + count);
        }
      }

    }

  }

  /**
   * Scanner for scanning index table.
   * @author wanhao
   */
  static class IndexSingleScanner extends SingleScanner {
    private boolean containAll = true;
    private Range[] range = null;
    private int flag;
    private IndexKeyGenerator kegGen;
    private FilterList ftlist;
    private byte[][] indexColumn;
    private boolean existKey = false;
    private int keyflag = -1;
    private int maxGetsPerScanner = 1;
    private GetThread[] threadPool = null;
    private int poolCounter = 0;
    private ArrayList<Result> resultPool = null;
    private ArrayList<HTable> tables = null;
    private boolean omitkv = false;

    public IndexSingleScanner(Scan scan, Range[] range, int flag, byte[][] resultColumns,
        HTable table, IndexKeyGenerator keyGen, IndexSpecification indexSpec, boolean containAll,
        byte[] mainTableName, FilterList list, int maxGets) throws IOException {
      super(scan, resultColumns, table);
      this.range = range;
      this.flag = flag;
      this.kegGen = keyGen;
      this.indexColumn = KeyValue.parseColumn(range[flag].getColumn());

      for (int i = 0; i < range.length; i++) {
        if (Bytes.compareTo(range[i].getColumn(), IndexConstants.KEY) == 0) {
          existKey = true;
          keyflag = i;
          break;
        }
      }

      this.containAll = containAll;
      this.ftlist = list;
      this.maxGetsPerScanner = maxGets;

      if (containAll == false) {
        threadPool = new GetThread[maxGetsPerScanner];
        poolCounter = 0;
        resultPool = new ArrayList<Result>();
        tables = new ArrayList<HTable>(maxGetsPerScanner);

        for (int i = 0; i < maxGetsPerScanner; i++) {
          tables.add(new HTable(table.getConfiguration(), mainTableName));
        }

      } else {
        if (indexSpec.getIndexType() == IndexType.SECONDARYINDEX
            || (indexSpec.getIndexType() == IndexType.IMPSECONDARYINDEX && indexSpec
                .getAdditionMap().size() == 0)
            || (resultColumns != null && resultColumns.length == 1 && flag >= 0 && Bytes.compareTo(
              range[flag].getColumn(), resultColumns[0]) == 0)) {
          omitkv = true;
        } else {
          omitkv = false;
        }
      }
    }

    public void run() {
      LOG.debug(this.getName() + "-" + getId() + " start scanning, startkey:"
          + Bytes.toStringBinary(scan.getStartRow()) + ", endkey:"
          + Bytes.toStringBinary(scan.getStopRow()));

      Result rs, rsnew;
      KeyValue tempkv = null;
      TreeSet<KeyValue> templist;
      byte[][] tempkey;

      while (!finished) {
        try {
          while ((rs = resultScanner.next()) != null && scanning) {
            tempkey = kegGen.parseIndexRowKey(rs.getRow());

            if (existKey) {
              if (range[keyflag].getStartValue() != null) {
                if (range[keyflag].getStartType() == CompareOp.GREATER) {
                  if (Bytes.compareTo(tempkey[0], range[keyflag].getStartValue()) <= 0) {
                    continue;
                  }
                } else if (range[keyflag].getStartType() == CompareOp.GREATER_OR_EQUAL) {
                  if (Bytes.compareTo(tempkey[0], range[keyflag].getStartValue()) < 0) {
                    continue;
                  }
                } else if (range[keyflag].getStartType() == CompareOp.EQUAL) {
                  if (Bytes.compareTo(tempkey[0], range[keyflag].getStartValue()) != 0) {
                    continue;
                  }
                }
              }

              if (range[keyflag].getEndValue() != null) {
                if (range[keyflag].getEndType() == CompareOp.LESS) {
                  if (Bytes.compareTo(tempkey[0], range[keyflag].getEndValue()) >= 0) {
                    continue;
                  }
                } else if (range[keyflag].getEndType() == CompareOp.LESS_OR_EQUAL) {
                  if (Bytes.compareTo(tempkey[0], range[keyflag].getEndValue()) > 0) {
                    continue;
                  }
                }
              }
            }

            if (range[flag].getStartValue() != null) {
              if (range[flag].getStartType() == CompareOp.GREATER) {
                if (Bytes.compareTo(tempkey[1], range[flag].getStartValue()) <= 0) {
                  continue;
                }
              } else if (range[flag].getStartType() == CompareOp.GREATER_OR_EQUAL) {
                if (Bytes.compareTo(tempkey[1], range[flag].getStartValue()) < 0) {
                  continue;
                }
              } else if (range[flag].getStartType() == CompareOp.EQUAL) {
                if (Bytes.compareTo(tempkey[1], range[flag].getStartValue()) != 0) {
                  continue;
                }
              }
            }

            if (range[flag].getEndValue() != null) {
              if (range[flag].getEndType() == CompareOp.LESS) {
                if (Bytes.compareTo(tempkey[1], range[flag].getEndValue()) >= 0) {
                  continue;
                }
              } else if (range[flag].getEndType() == CompareOp.LESS_OR_EQUAL) {
                if (Bytes.compareTo(tempkey[1], range[flag].getEndValue()) > 0) {
                  continue;
                }
              }
            }

            // System.out.println(rs);
            if (containAll) {
              templist = new TreeSet<KeyValue>(KeyValue.COMPARATOR);

              for (KeyValue kv : rs.raw()) {
                tempkv =
                    new KeyValue(tempkey[0], kv.getFamily(), kv.getQualifier(), kv.getTimestamp(),
                        kv.getValue());
                if (omitkv) {
                  continue;
                } // omit kv whose qualifier is null
                templist.add(tempkv);
              }

              tempkv =
                  new KeyValue(tempkey[0], indexColumn[0], indexColumn[1], tempkv.getTimestamp(),
                      tempkey[1]);
              templist.add(tempkv);
              rsnew = new Result(templist.toArray(new KeyValue[0]));

              if (!rsnew.isEmpty()) {
                while (!resultBuffer.offer(rsnew, 10, TimeUnit.MILLISECONDS)) {
                  if (!scanning) {
                    break;
                  }
                }
              }
              currentStartKey = rs.getRow();
              count++;

            } else {
              Get get = new Get(tempkey[0]);
              get.setFilter(ftlist);
              get.setCacheBlocks(false);

              if (resultColumns != null && resultColumns.length != 0) {
                for (byte[] column : resultColumns) {
                  byte[][] tmp = KeyValue.parseColumn(column);

                  if (tmp.length == 1) {
                    get.addFamily(tmp[0]);
                  } else {
                    get.addColumn(tmp[0], tmp[1]);
                  }
                }
              }

              rsnew = tables.get(poolCounter).get(get);

              if (!rsnew.isEmpty()) {
                while (!resultBuffer.offer(rsnew, 10, TimeUnit.MILLISECONDS)) {
                  if (!scanning) {
                    break;
                  }
                }
                if (!scanning) {
                  break;
                }

              }

              currentStartKey = rs.getRow();
              count++;
            }

          }

          if (!containAll) {
            if (poolCounter != 0) {
              for (int i = 0; i < poolCounter; i++) {
                threadPool[i].join();
                rsnew = threadPool[i].getResult();
                if (rsnew != null) {
                  if (!rsnew.isEmpty()) {
                    resultPool.add(rsnew);
                  }
                } else {
                  throw threadPool[i].getException();
                }
              }

              if (!resultPool.isEmpty()) {
                for (Result tmp : resultPool) {
                  while (!resultBuffer.offer(tmp, 10, TimeUnit.MILLISECONDS)) {
                    if (!scanning) {
                      break;
                    }
                  }
                  if (!scanning) {
                    break;
                  }
                }
              }
              count += resultPool.size();
            }
          }

          if (scanning) {
            finished = true;
          }
        } catch (Exception e) {
          LOG.warn(this.getName() + "-" + getId() + " stop scanning!", e);
        } finally {
          if (!containAll) {
            if (poolCounter != 0) {
              for (int i = 0; i < poolCounter; i++) {
                while (threadPool[i].isAlive()) {
                  try {
                    threadPool[i].join();
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }
              }

            }
            poolCounter = 0;
          }
          resultScanner.close();
        }

        if (!finished) {
          scanning = false;
          LOG.debug(this.getName() + "-" + getId() + " stop scanning, waiting to be restarted!");
          indexResultScanner.addStopped(getName());
          LOG.debug(this.getName() + "-" + getId() + " is waked up, restart scan!");
        }

        if (finished) {
          indexResultScanner.addFinished(getName());
          LOG.debug(this.getName() + "-" + getId() + " finish scanning," + " total records:"
              + count);
        }
      }

    }

    class GetThread extends Thread {
      private FilterList ftlist;
      private byte[][] resultColumns;
      private HTable table;
      private byte[] rowkey;
      private Result rsnew = null;
      private Exception exception = null;

      public GetThread(FilterList ftlist, byte[][] resultColumns, HTable table, byte[] rowkey) {
        this.ftlist = ftlist;
        this.resultColumns = resultColumns;
        this.table = table;
        this.rowkey = rowkey;
      }

      public void run() {
        try {
          Get get = new Get(rowkey);
          get.setFilter(ftlist);
          get.setCacheBlocks(false);

          if (resultColumns != null && resultColumns.length != 0) {
            for (byte[] column : resultColumns) {
              byte[][] tmp = KeyValue.parseColumn(column);

              if (tmp.length == 1) {
                get.addFamily(tmp[0]);
              } else {
                get.addColumn(tmp[0], tmp[1]);
              }
            }
          }

          rsnew = table.get(get);
          table.close();
        } catch (Exception e) {
          rsnew = null;
          exception = e;
        }
      }

      public Result getResult() {
        return rsnew;
      }

      public Exception getException() {
        return exception;
      }
    }
  }

}
