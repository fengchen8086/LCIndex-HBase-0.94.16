package doWork;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.util.Bytes;

// winter added class
public class LCCIndexGenerator {
  // rawkey <<-->> all inserted index keys
  private Map<byte[], ArrayList<byte[]>> lccRowkeyMap;
  private ArrayList<KeyValue> lccResults;
  private TreeMap<byte[], DataType> lccIndexQualifierType;
  // rawQualifier <<-->> number of qualifier
  private Map<byte[], Integer> lccIndexStatistic;
  private Queue<KeyValue> lccQueue = null;
  List<LCStatInfo> statList = null;

  public LCCIndexGenerator(TreeMap<byte[], DataType> lccIndexQualifier, String descStr) {
    lccRowkeyMap = new TreeMap<byte[], ArrayList<byte[]>>(Bytes.BYTES_COMPARATOR);
    lccIndexStatistic = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    lccResults = new ArrayList<KeyValue>();
    this.lccIndexQualifierType = lccIndexQualifier;
    lccQueue = new LinkedList<KeyValue>();
    if (descStr != null) {
      try {
        statList = LCStatInfo.parseStatString(descStr);
      } catch (IOException e) {
        System.out.println("winter can not parse string to LCStatInfo, str: " + descStr);
        System.out.println("winter can not parse string to LCStatInfo, exception msg: " + e);
      }
    } else {
      System.out.println("winter will not create statList because desc is null");
    }
  }

  public void processKeyValue(KeyValue kv) throws IOException {
    if (statList != null) updateLCRangeStat(kv);
    if (mWinterIsLCCIndexColumn(kv.getQualifier())) {
      mWinterProcessOneKeyValue(kv);
    } else {
      lccQueue.add(kv);
    }
  }

  private void updateLCRangeStat(KeyValue kv) throws IOException {
    if (mWinterIsLCCIndexColumn(kv.getQualifier())) {
      for (LCStatInfo stat : statList) {
        if (Bytes.equals(stat.getQualifier(), kv.getQualifier())) {
          stat.updateRange(kv.getValue());
        }
      }
    }
  }

  private void updateStatistic(String str) {
    updateStatistic(Bytes.toBytes(str));
  }

  private void updateStatistic(byte[] qualifer) {
    if (mWinterIsLCCIndexColumn(qualifer)) {
      // qualifer = Bytes.toBytes("single");
      if (lccIndexStatistic.containsKey(qualifer)) {
        lccIndexStatistic.put(qualifer, lccIndexStatistic.get(qualifer) + 1);
      } else {
        lccIndexStatistic.put(qualifer, 1);
      }
    }
  }

  private void mWinterProcessOneKeyValue(KeyValue kv) {
    if (lccRowkeyMap.containsKey(kv.getRow())) {
      // already has the rowkey, first get the list
      ArrayList<byte[]> listOfIndexedKeys = lccRowkeyMap.get(kv.getRow());
      for (byte[] indexedKeyBytes : listOfIndexedKeys) {
        // use indexedKeyBytes as used key
        lccResults.add(mWinterGenerateLCCKeyValue(indexedKeyBytes, kv.getQualifier(),
          kv.getValue(), kv.getTimestamp()));
        updateStatistic(Bytes.toString(indexedKeyBytes).split(LCCIndexConstant.DELIMITER_STR)[1]);
        if (mWinterIsLCCIndexColumn(kv.getQualifier())) {
          // use kv as new index key
          lccResults.add(mWinterGenerateKeyValueFromExistingKey(mWinterGetLCCIndexKey(kv),
            indexedKeyBytes, kv.getQualifier(), kv.getTimestamp()));
          updateStatistic(kv.getQualifier());
        }
      }
      if (mWinterIsLCCIndexColumn(kv.getQualifier())) { // add new KeyValue back
        listOfIndexedKeys.add(mWinterGetLCCIndexKey(kv));
      }
    } else { // new added rowkey
      ArrayList<byte[]> listOfIndexedKeys = new ArrayList<byte[]>();
      listOfIndexedKeys.add(mWinterGetLCCIndexKey(kv));
      lccRowkeyMap.put(kv.getRow(), listOfIndexedKeys);
    }
  }

  // key = key // targetKey = idx_V#Qualifier#RawKey
  // family = if_ + rawQualifier
  // new qualifier = parsed from key
  // value = idx_V parsed fom key
  private KeyValue mWinterGenerateKeyValueFromExistingKey(byte[] key, byte[] indexedKey,
      byte[] rawQualifier, long ts) {
    String temp[] = Bytes.toString(indexedKey).split(LCCIndexConstant.DELIMITER_STR);
    byte[] value = null;
    value =
        LCCIndexConstant.parsingStringToBytes(lccIndexQualifierType, Bytes.toBytes(temp[1]),
          temp[0]);
    return new KeyValue(key, mWinterGenerateLCCIndexFamily_Bytes(rawQualifier),
        Bytes.toBytes(temp[1]), ts, KeyValue.Type.Put, value);
  }

  // key = targetKey // targetKey = idx_V#Qualifier#RawKey
  // family = parsed from targetKey
  // rawQualifier = rawQualifier
  // value = new Value
  private KeyValue mWinterGenerateLCCKeyValue(byte[] targetKey, byte[] rawQualifier, byte[] value,
      long ts) {
    String temp[] = Bytes.toString(targetKey).split(LCCIndexConstant.DELIMITER_STR);
    return new KeyValue(targetKey, mWinterGenerateLCCIndexFamily_Str(temp[1]), rawQualifier, ts,
        KeyValue.Type.Put, value);
  }

  public static byte[] mWinterGenerateLCCIndexFamily_Str(String rawFamily) {
    return Bytes.toBytes(LCCIndexConstant.CF_FAMILY_PREFIX_STR + rawFamily);
  }

  public static byte[] mWinterGenerateLCCIndexFamily_Bytes(byte[] target) {
    return Bytes.add(LCCIndexConstant.CF_FAMILY_PREFIX_BYTES, target);
  }

  public static String mWinterRecoverLCCQualifier_Str(String indexFamilyName) {
    return indexFamilyName.substring(LCCIndexConstant.CF_FAMILY_PREFIX_STR.length());
  }

  // winter function
  private boolean mWinterIsLCCIndexColumn(byte[] qualifier) {
    return lccIndexQualifierType.containsKey(qualifier);
  }

  // value#qualifier#rawkey
  private byte[] mWinterGetLCCIndexKey(KeyValue kv) {
    String valueStr = null;
    // force here, should be modified later
    // value#qualifier#rowkey
    switch (LCCIndexConstant.getType(lccIndexQualifierType, kv.getQualifier())) {
    case DOUBLE:
      valueStr = LCCIndexConstant.paddedStringDouble(Bytes.toDouble(kv.getValue()));
      break;
    case INT:
      valueStr = LCCIndexConstant.paddedStringInt(Bytes.toInt(kv.getValue()));
      break;
    case LONG:
      valueStr = LCCIndexConstant.paddedStringLong(Bytes.toLong(kv.getValue()));
      break;
    case SHORT:
      valueStr = LCCIndexConstant.paddedStringShort(Bytes.toShort(kv.getValue()));
      break;
    case STRING:
      // valueStr = LCCIndexConstant.paddedStringString(Bytes.toString(kv.getValue()));
      valueStr = Bytes.toString(kv.getValue());
      break;
    default:
      System.out.println("winter unsupported type in lccindex generator");
      break;
    }
    return Bytes.add(
      Bytes.add(Bytes.toBytes(valueStr), LCCIndexConstant.DELIMITER_BYTES, kv.getQualifier()),
      LCCIndexConstant.DELIMITER_BYTES, kv.getRow());
  }

  public KeyValue[] generatedSortedKeyValueArray() {
    while (!lccQueue.isEmpty()) {
      mWinterProcessOneKeyValue(lccQueue.poll());
    }
    if (lccResults.size() == 0) {
      return null;
    }
    KeyValue[] array = lccResults.toArray(new KeyValue[lccResults.size()]);
    Arrays.sort(array, KeyValue.COMPARATOR);
    return array;
  }

  public Map<byte[], Integer> getResultStatistic() {
    return lccIndexStatistic;
  }

  public List<LCStatInfo> getLCRangeStatistic() {
    return statList;
  }

  public void clear() {
    lccRowkeyMap = null;
    lccResults = null;
    lccIndexQualifierType = null;
    lccQueue = null;
    lccIndexStatistic = null;
    statList = null;
  }
}
