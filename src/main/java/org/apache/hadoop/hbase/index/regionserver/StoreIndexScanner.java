package org.apache.hadoop.hbase.index.regionserver;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.index.regionserver.IndexKeyValue.IndexKVComparator;
import org.apache.hadoop.hbase.regionserver.ChangedReadersObserver;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;

public class StoreIndexScanner implements ChangedReadersObserver {
  private Store store;
  private Range range;
  private volatile boolean needToRefresh = false;
  private Set<ByteArray> indexSet;
  private Set<ByteArray> joinSet;
  private boolean isEmptySet;
  private boolean isAND;

  private byte[] startRow = null;
  private byte[] stopRow = null;
  private KeyValue startKV = null;
  private KeyValue stopKV = null;
  private int stopRowCmpValue;

  private IndexKeyValue startIKV = null;
  private IndexKeyValue stopIKV = null;
  private int stopIKVCmpValue;

  private boolean cacheBlocks;
  private boolean isGet;
  private KVComparator comparator;
  private IndexKVComparator indexComparator;

  private List<StoreFileIndexScanner> storeFileIndexScanners;
  private List<KeyValueScanner> memstoreScanner;

  public StoreIndexScanner(Store store, List<KeyValueScanner> scanners, KVComparator comparator,
      IndexKVComparator indexComparator, Range range, Scan scan, Set<ByteArray> joinSet,
      boolean isAND) throws IOException {
    // winter scanner is always 1? in my test it is 1 indeed
    this.store = store;
    this.joinSet = joinSet;
    this.isAND = isAND;
    this.memstoreScanner = scanners;
    this.comparator = comparator;
    this.indexComparator = indexComparator;
    this.range = range;
    this.isGet = scan.isGetScan();
    this.cacheBlocks = scan.getCacheBlocks();
    if (isAND) {
      this.isEmptySet = this.joinSet.isEmpty();
      this.indexSet = new HashSet<ByteArray>(10000);
    }
    this.startRow = scan.getStartRow();
    this.startKV = KeyValue.createFirstOnRow(startRow);
    this.stopRow =
        Bytes.compareTo(scan.getStopRow(), HConstants.EMPTY_BYTE_ARRAY) == 0 ? null : scan
            .getStopRow();
    this.stopKV =
        Bytes.compareTo(scan.getStopRow(), HConstants.EMPTY_BYTE_ARRAY) == 0 ? null : KeyValue
            .createLastOnRow(scan.getStopRow());
    this.stopRowCmpValue = scan.isGetScan() ? -1 : 0;

    if (range.getStartValue() != null) {
      switch (range.getStartType()) {
      case EQUAL:
        startIKV =
            IndexKeyValue.createFirstOnQualifier(range.getQualifier(), range.getStartValue());
        stopIKV = startIKV;
        stopIKVCmpValue = -1;
        break;
      case GREATER_OR_EQUAL:
        startIKV =
            IndexKeyValue.createFirstOnQualifier(range.getQualifier(), range.getStartValue());
        stopIKV = null;
        stopIKVCmpValue = 0;
        break;
      case GREATER:
        startIKV = IndexKeyValue.createLastOnQualifier(range.getQualifier(), range.getStartValue());
        stopIKV = null;
        stopIKVCmpValue = 0;
        break;
      default:
        throw new IOException("Invalid Range:" + range);
      }
    } else {
      startIKV = IndexKeyValue.createFirstOnQualifier(range.getQualifier());
      stopIKV = null;
    }

    if (range.getStopValue() != null) {
      switch (range.getStopType()) {
      case LESS:
        stopIKV = IndexKeyValue.createFirstOnQualifier(range.getQualifier(), range.getStopValue());
        stopIKVCmpValue = 0;
        break;
      case LESS_OR_EQUAL:
        stopIKV = IndexKeyValue.createFirstOnQualifier(range.getQualifier(), range.getStopValue());
        stopIKVCmpValue = -1;
        break;
      default:
        throw new IOException("Invalid Range:" + range);
      }
    }

    this.needToRefresh = false;
    getScanners();
  }

  public Set<ByteArray> doScan() throws IOException {
    for (int i = 0; i < storeFileIndexScanners.size() + memstoreScanner.size(); i++) {
      if (needToRefresh) {
        indexSet.clear();
        getScanners();
        i = 0;
      }
      if (i < storeFileIndexScanners.size()) {
        while (nextFromIndexFile(storeFileIndexScanners.get(i), 100)) {
        }
      } else {
        while (nextFromMemStore(memstoreScanner.get(i - storeFileIndexScanners.size()), 100)) {
        }
      }
    }
    if (this.isAND) {
      return this.indexSet;
    } else {
      return this.joinSet;
    }
  }

  public byte[][] getStoreIndexHeap() {
    byte[][] heap = this.indexSet.toArray(new byte[indexSet.size()][]);
    QuickSort.sort(heap, Bytes.BYTES_COMPARATOR);
    return heap;
  }

  public void getScanners() throws IOException {
    storeFileIndexScanners =
        StoreFileIndexScanner.getScannersForStoreFiles(store.getStorefiles(), cacheBlocks, isGet);
    for (StoreFileIndexScanner scanner : storeFileIndexScanners) {
      scanner.seek(startIKV);
    }
    for (KeyValueScanner scanner : memstoreScanner) {
      scanner.seek(startKV);
    }
  }

  public synchronized void close() {
    if (this.store != null) this.store.deleteChangedReaderObserver(this);
    for (StoreFileIndexScanner scanner : storeFileIndexScanners) {
      scanner.close();
    }
    for (KeyValueScanner scanner : memstoreScanner) {
      scanner.close();
    }
  }

  public synchronized void updateReaders() throws IOException {
    needToRefresh = true;
  }

  public synchronized boolean nextFromMemStore(KeyValueScanner scanner, int interval)
      throws IOException {
    int count = 0;
    KeyValue currentKV = null;
    ByteArray currentRow = null;
    while (true) {
      count++;
      if (count > interval) {
        return true;
      }

      if ((currentKV = scanner.next()) == null) {
        return false;
      }

      if (shouldStop(currentKV)) {
        return false;
      }

      if (!filterKVByQualifier(currentKV)) {
        currentRow =
            new ByteArray(currentKV.getBuffer(), currentKV.getRowOffset(), currentKV.getRowLength());
        if (isAND) {
          if (this.isEmptySet || joinSet.contains(currentRow)) {
            indexSet.add(currentRow);
          }
        } else {
          joinSet.add(currentRow);
        }
      }
    }
  }

  public synchronized boolean nextFromIndexFile(StoreFileIndexScanner scanner, int interval)
      throws IOException {
    int count = 0;
    IndexKeyValue currentIKV = null;
    ByteArray currentRow = null;
    while (true) {
      count++;
      if (count > interval) {
        return true;
      }
      if ((currentIKV = scanner.next()) == null) {
        return false; // to the end of the scanner
      }
      if (shouldStop(currentIKV)) {
        return false;
      }
      if (!filterIKVByRow(currentIKV)) {
        currentRow =
            new ByteArray(currentIKV.getBuffer(), currentIKV.getRowOffset(),
                currentIKV.getRowLength());
        if (isAND) {
          if (this.isEmptySet || joinSet.contains(currentRow)) {
            indexSet.add(currentRow);
          }
        } else {
          joinSet.add(currentRow);
        }
      }

    }
  }

  private boolean shouldStop(IndexKeyValue currentIKV) {
    if (stopIKV != null && indexComparator.compareKey(stopIKV, currentIKV) <= stopIKVCmpValue) {
      return true;
    }
    return false;
  }

  private boolean shouldStop(KeyValue currentKV) {
    if (stopKV != null && comparator.compare(stopKV, currentKV) <= stopRowCmpValue) {
      return true;
    }
    return false;
  }

  private boolean filterIKVByRow(IndexKeyValue currentIKV) {
    if (Bytes.compareTo(currentIKV.getBuffer(), currentIKV.getRowOffset(),
      currentIKV.getRowLength(), startRow, 0, startRow.length) < 0) {
      return true;
    }

    if (stopRow != null
        && Bytes.compareTo(stopRow, 0, stopRow.length, currentIKV.getBuffer(),
          currentIKV.getRowOffset(), currentIKV.getRowLength()) <= stopRowCmpValue) {
      return true;
    }

    return false;
  }

  private boolean filterKVByQualifier(KeyValue currentKV) {
    if (Bytes.compareTo(range.getQualifier(), 0, range.getQualifier().length,
      currentKV.getBuffer(), currentKV.getQualifierOffset(), currentKV.getQualifierLength()) != 0) {
      return true;
    }

    if (range.getStartValue() != null) {
      switch (range.getStartType()) {
      case EQUAL:
        if (Bytes.compareTo(currentKV.getBuffer(), currentKV.getValueOffset(),
          currentKV.getValueLength(), range.getStartValue(), 0, range.getStartValue().length) != 0) {
          return true;
        }
        break;
      case GREATER_OR_EQUAL:
        if (Bytes.compareTo(currentKV.getBuffer(), currentKV.getValueOffset(),
          currentKV.getValueLength(), range.getStartValue(), 0, range.getStartValue().length) < 0) {
          return true;
        }
        break;
      case GREATER:
        if (Bytes.compareTo(currentKV.getBuffer(), currentKV.getValueOffset(),
          currentKV.getValueLength(), range.getStartValue(), 0, range.getStartValue().length) <= 0) {
          return true;
        }
        break;
      default:
        break;
      }
    }

    if (range.getStopValue() != null) {
      switch (range.getStopType()) {
      case LESS:
        if (Bytes.compareTo(currentKV.getBuffer(), currentKV.getValueOffset(),
          currentKV.getValueLength(), range.getStopValue(), 0, range.getStopValue().length) >= 0) {
          return true;
        }
        break;
      case LESS_OR_EQUAL:
        if (Bytes.compareTo(currentKV.getBuffer(), currentKV.getValueOffset(),
          currentKV.getValueLength(), range.getStopValue(), 0, range.getStopValue().length) > 0) {
          return true;
        }
        break;
      default:
        break;
      }
    }

    return false;
  }
}
