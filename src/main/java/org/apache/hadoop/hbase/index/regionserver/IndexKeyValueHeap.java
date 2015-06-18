package org.apache.hadoop.hbase.index.regionserver;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.hbase.index.regionserver.IndexKeyValue.IndexKVComparator;

/**
 * Implements a heap merge across any number of IndexKeyValueScanners.
 * <p>
 * Implements IndexKeyValueScanner itself.
 * <p>
 * This class is used at the Store level to merge across IndexStoreFiles.
 */
public class IndexKeyValueHeap implements IndexKeyValueScanner {
  private PriorityQueue<IndexKeyValueScanner> heap = null;
  private IndexKeyValueScanner current = null;
  private IKVScannerComparator comparator;

  /**
   * Constructor.  This IndexKeyValueHeap will handle closing of passed in
   * IndexKeyValueScanners.
   * @param scanners
   * @param comparator
   */
  public IndexKeyValueHeap(List<? extends IndexKeyValueScanner> scanners,
      IndexKVComparator comparator) {
    this.comparator = new IKVScannerComparator(comparator);
    if (!scanners.isEmpty()) {
      this.heap = new PriorityQueue<IndexKeyValueScanner>(scanners.size(),
          this.comparator);
      for (IndexKeyValueScanner scanner : scanners) {
        if (scanner.peek() != null) {
          this.heap.add(scanner);
        } else {
          scanner.close();
        }
      }
      this.current = heap.poll();
    }
  }

  public IndexKeyValue peek() {
    if (this.current == null) {
      return null;
    }
    return this.current.peek();
  }

  public IndexKeyValue next()  throws IOException {
    if(this.current == null) {
      return null;
    }
    IndexKeyValue kvReturn = this.current.next();
    IndexKeyValue kvNext = this.current.peek();
    if (kvNext == null) {
      this.current.close();
      this.current = this.heap.poll();
    } else {
      IndexKeyValueScanner topScanner = this.heap.peek();
      if (topScanner == null ||
          this.comparator.compare(kvNext, topScanner.peek()) >= 0) {
        this.heap.add(this.current);
        this.current = this.heap.poll();
      }
    }
    return kvReturn;
  }

  private static class IKVScannerComparator implements Comparator<IndexKeyValueScanner> {
    private IndexKVComparator kvComparator;
    /**
     * Constructor
     * @param kvComparator
     */
    public IKVScannerComparator(IndexKVComparator kvComparator) {
      this.kvComparator = kvComparator;
    }
    public int compare(IndexKeyValueScanner left, IndexKeyValueScanner right) {
      int comparison = compare(left.peek(), right.peek());
      if (comparison != 0) {
        return comparison;
      } else {
        // Since both the keys are exactly the same, we break the tie in favor
        // of the key which came latest.
        long leftSequenceID = left.getSequenceID();
        long rightSequenceID = right.getSequenceID();
        if (leftSequenceID > rightSequenceID) {
          return -1;
        } else if (leftSequenceID < rightSequenceID) {
          return 1;
        } else {
          return 0;
        }
      }
    }
    /**
     * Compares two IndexKeyValue
     * @param left
     * @param right
     * @return less than 0 if left is smaller, 0 if equal etc..
     */
    public int compare(IndexKeyValue left, IndexKeyValue right) {
      return this.kvComparator.compare(left, right);
    }

    public IndexKVComparator getComparator() {
      return this.kvComparator;
    }
  }

  public void close() {
    if (this.current != null) {
      this.current.close();
    }
    if (this.heap != null) {
      IndexKeyValueScanner scanner;
      while ((scanner = this.heap.poll()) != null) {
        scanner.close();
      }
    }
  }

  /**
   * Seeks all scanners at or below the specified seek key.  If we earlied-out
   * of a row, we may end up skipping values that were never reached yet.
   * Rather than iterating down, we want to give the opportunity to re-seek.
   * <p>
   * As individual scanners may run past their ends, those scanners are
   * automatically closed and removed from the heap.
   * @param seekKey IndexKeyValue to seek at or after
   * @return true if IndexKeyValues exist at or after specified key, false if not
   * @throws IOException
   */
  public boolean seek(IndexKeyValue seekKey) throws IOException {
    if (this.current == null) {
      return false;
    }
    this.heap.add(this.current);
    this.current = null;

    IndexKeyValueScanner scanner;
    while((scanner = this.heap.poll()) != null) {
      IndexKeyValue topKey = scanner.peek();
      if(comparator.getComparator().compare(seekKey, topKey) <= 0) { // Correct?
        // Top IndexKeyValue is at-or-after Seek IndexKeyValue
        this.current = scanner;
        return true;
      }
      if(!scanner.seek(seekKey)) {
        scanner.close();
      } else {
        this.heap.add(scanner);
      }
    }
    // Heap is returning empty, scanner is done
    return false;
  }

  public boolean reseek(IndexKeyValue seekKey) throws IOException {
    //This function is very identical to the seek(IndexKeyValue) function except that
    //scanner.seek(seekKey) is changed to scanner.reseek(seekKey)
    if (this.current == null) {
      return false;
    }
    this.heap.add(this.current);
    this.current = null;

    IndexKeyValueScanner scanner;
    while ((scanner = this.heap.poll()) != null) {
      IndexKeyValue topKey = scanner.peek();
      if (comparator.getComparator().compare(seekKey, topKey) <= 0) {
        // Top IndexKeyValue is at-or-after Seek IndexKeyValue
        this.current = scanner;
        return true;
      }
      if (!scanner.reseek(seekKey)) {
        scanner.close();
      } else {
        this.heap.add(scanner);
      }
    }
    // Heap is returning empty, scanner is done
    return false;
  }

  /**
   * @return the current Heap
   */
  public PriorityQueue<IndexKeyValueScanner> getHeap() {
    return this.heap;
  }

  @Override
  public long getSequenceID() {
    return 0;
  }
}
