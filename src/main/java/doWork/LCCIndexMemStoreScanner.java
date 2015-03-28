package doWork;

import java.io.IOException;
import java.util.LinkedList;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.NonLazyKeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;

// winter newly added class
public class LCCIndexMemStoreScanner extends NonLazyKeyValueScanner {

  // demands:
  // 1. fast peek and seek
  // 2. fast insert, when insert happens, little data are moved

  // private final long UPDATE_THRESHOLD = 1000;

  // winter use a hash function to locate the rowkey into different rows

  // optimization needed but not now!
  LinkedList<KeyValue> dataList;

  int currentIndexPoint;
  TreeMap<byte[], DataType> lccIndexQualifier;

  public LCCIndexMemStoreScanner(KeyValueScanner scanner,
      TreeMap<byte[], DataType> lccIndexQualifier, byte[] target) throws IOException {
    super();
    dataList = new LinkedList<KeyValue>();
    this.lccIndexQualifier = new TreeMap<byte[], DataType>(Bytes.BYTES_COMPARATOR);
    if (target == null || !lccIndexQualifier.containsKey(target)) {
      throw new IOException("winter index column " + Bytes.toString(target) + " is uknown type");
    }
    this.lccIndexQualifier.put(target, lccIndexQualifier.get(target));
    // this.lccIndexQualifier = lccIndexQualifier;
    long start = System.currentTimeMillis();
    init(scanner);
    System.out.println("winter LCCIndexMemStoreScanner cost "
        + (System.currentTimeMillis() - start) / 1000.0
        + " seconds to build lcc memstore scanner from memstore, the size of this scanner is: "
        + dataList.size());
  }

  private void init(KeyValueScanner scanner) {
    KeyValue kv;
    LCCIndexGenerator generator = new LCCIndexGenerator(lccIndexQualifier, null);
    try {
      while ((kv = scanner.next()) != null) {
        generator.processKeyValue(kv);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    KeyValue[] kvArray = generator.generatedSortedKeyValueArray();
    if (kvArray != null && kvArray.length != 0) {
      for (KeyValue t : kvArray) {
        dataList.add(t);
      }
    }
    currentIndexPoint = 0;
  }

  @Override
  public synchronized KeyValue peek() {
    // System.out.println("winter now index point: " + currentIndexPoint + " total size: "
    // + dataList.size());
    if (currentIndexPoint >= dataList.size()) {
      return null;
    }
    return dataList.get(currentIndexPoint);
  }

  @Override
  public synchronized KeyValue next() throws IOException {
    if (currentIndexPoint >= dataList.size()) {
      return null;
    }
    final KeyValue ret = dataList.get(currentIndexPoint);
    ++currentIndexPoint;
    return ret;
  }

  @Override
  public synchronized boolean seek(KeyValue key) throws IOException {
    // return the
    if (key == null) {
      close();
      return false;
    }
    // if the current key is greater than the target, return false
    if (currentIndexPoint >= dataList.size()
        || Bytes.compareTo(key.getKey(), dataList.get(currentIndexPoint).getKey()) <= 0) {
      // System.out
      // .println("winter testing lccindex memstore scanner, current is greater than the target, return false");
      return false;
    }

    while (currentIndexPoint < dataList.size()) {
      // the key now pointed should be right bigger than the target
      // return the first point where the value is greater than the target
      if (Bytes.compareTo(key.getKey(), dataList.get(currentIndexPoint).getKey()) <= 0) {
        System.out.println("winter testing lccindex memstore scanner, seek return true");
        return true;
      }
      ++currentIndexPoint;
    }
    return false;
  }

  @Override
  public synchronized boolean reseek(KeyValue key) throws IOException {
    if (key == null) {
      close();
      return false;
    }
    // if the target is greater than the max, return false
    if (dataList.size() == 0 || Bytes.compareTo(key.getKey(), dataList.getLast().getKey()) > 0) {
      return false;
    }
    currentIndexPoint = 0;
    while (currentIndexPoint < dataList.size()) {
      // the key now pointed should be right bigger than the target
      // return the first point where the value is greater than the target
      // int cmp = Bytes.compareTo(key.getKey(), dataList.get(currentIndexPoint).getKey());
      if (KeyValue.COMPARATOR.compare(key, dataList.get(currentIndexPoint)) <= 0) {
        return true;
      }
      ++currentIndexPoint;
    }
    return false;
  }

  /**
   * MemStoreScanner returns max value as sequence id because it will always have the latest data
   * among all files. LCCIndexMemStoreScanner return MAX-1 to diff
   */
  @Override
  public synchronized long getSequenceID() {
    return Long.MAX_VALUE - 1;
  }

  @Override
  public synchronized void close() {
    // TODO Auto-generated method stub
    dataList.clear();
    dataList = null;
    currentIndexPoint = 0;
  }
}
