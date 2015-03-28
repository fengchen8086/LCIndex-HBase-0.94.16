package doWork;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.util.Bytes;

public class LCStatInfo {
  public final int LC_STATISTIC_PARTS = 1000;
  private String name;
  private byte[] qualifier = null;
  private DataType type;
  private boolean isSet;
  private byte[][] keys = null;
  private long[] counts = null;
  private TreeMap<byte[], Long> setMap = null;

  public LCStatInfo(String name, DataType type, String[] allParts, int startIndex)
      throws IOException {
    this.name = name;
    this.type = type;
    this.isSet = true;
    setMap = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    for (int i = startIndex; i < allParts.length; ++i) {
      setMap.put(LCCIndexConstant.parsingStringToBytesWithType(type, allParts[i]), (long) 0);
    }
  }

  public LCStatInfo(String name, DataType type, int parts, String min, String max)
      throws IOException {
    this.name = name;
    this.type = type;
    this.isSet = false;
    switch (type) {
    case INT:
      parseInt(parts, min, max);
      break;
    case LONG:
      parseLong(parts, min, max);
      break;
    case DOUBLE:
      parseDouble(parts, min, max);
      break;
    default:
      new Exception("winter StatInfo ranges not supportted type: " + type).printStackTrace();
      throw new IOException("winter StatInfo ranges not supportted type: " + type);
    }
  }

  private void initKV(int parts) {
    keys = new byte[parts + 1][];
    counts = new long[parts + 1];
  }

  private void parseInt(int parts, String minStr, String maxStr) {
    int min = Integer.valueOf(minStr);
    int max = Integer.valueOf(maxStr);
    if ((max - min) < parts) {
      parts = max - min;
    }
    initKV(parts);
    int interval = (max - min) / parts;
    int left = (max - min) % parts;
    for (int i = 0; i < parts; ++i) {
      int addition = i < left ? 1 : 0;
      keys[i] = Bytes.toBytes(min + i * interval + addition);
      counts[i] = 0;
    }
    keys[parts] = Bytes.toBytes(Integer.MAX_VALUE);
    counts[parts] = 0;
  }

  private void parseLong(int parts, String minStr, String maxStr) {
    long min = Long.valueOf(minStr);
    long max = Long.valueOf(maxStr);
    if ((max - min) < parts) {
      parts = (int) (max - min);
    }
    initKV(parts);
    long interval = (max - min) / parts;
    long left = (max - min) % parts;
    for (int i = 0; i < parts; ++i) {
      int addition = i < left ? 1 : 0;
      keys[i] = Bytes.toBytes(min + i * interval + addition);
      counts[i] = 0;
    }
    keys[parts] = Bytes.toBytes(Long.MAX_VALUE);
    counts[parts] = 0;
  }

  private void parseDouble(int parts, String minStr, String maxStr) {
    double min = Double.valueOf(minStr);
    double max = Double.valueOf(maxStr);
    initKV(parts);
    double interval = (max - min) / parts;
    for (int i = 0; i < parts; ++i) {
      keys[i] = Bytes.toBytes(min + i * interval);
      counts[i] = 0;
    }
    keys[parts] = Bytes.toBytes(Double.MAX_VALUE);
    counts[parts] = 0;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(name).append(",").append(type);
    if (isSet) {
      sb.append(",set values:");
      for (Entry<byte[], Long> entry : setMap.entrySet()) {
        sb.append(LCCIndexConstant.getStringOfValueAndType(type, entry.getKey())).append(":")
            .append(entry.getValue()).append("###");
      }
    } else {
      sb.append(",range values:");
      for (int i = 0; i < keys.length; ++i) {
        sb.append(LCCIndexConstant.getStringOfValueAndType(type, keys[i])).append(":")
            .append(counts[i]).append("###");
      }
    }
    return sb.toString();
  }

  public String getValuedString() {
    StringBuilder sb = new StringBuilder();
    sb.append(name).append(",").append(type);
    if (isSet) {
      sb.append(",set values:");
      for (Entry<byte[], Long> entry : setMap.entrySet()) {
        if (entry.getValue() > 0) {
          sb.append(LCCIndexConstant.getStringOfValueAndType(type, entry.getKey())).append(":")
              .append(entry.getValue()).append("###");
        }
      }
    } else {
      sb.append(",range values:");
      for (int i = 0; i < keys.length; ++i) {
        if (counts[i] > 0) {
          sb.append(LCCIndexConstant.getStringOfValueAndType(type, keys[i])).append(":")
              .append(counts[i]).append("###");
        }
      }
    }
    return sb.toString();
  }

  public static List<LCStatInfo> parseStatString(String str) throws IOException {
    return parseStatString(str, null);
  }

  public static List<LCStatInfo> parseStatString(String str, List<String> includedQualifiers)
      throws IOException {
    List<LCStatInfo> list = new ArrayList<LCStatInfo>();
    String[] lines = str.split(LCCIndexConstant.LCC_TABLE_DESC_RANGE_DELIMITER);
    for (String line : lines) {
      String[] parts = line.split("\t");
      if (includedQualifiers != null && !includedQualifiers.contains(parts[0])) {
        continue;
      }
      if ("set".equalsIgnoreCase(parts[2])) {
        list.add(new LCStatInfo(parts[0], DataType.valueOf(parts[1]), parts, 3));
      } else {
        list.add(new LCStatInfo(parts[0], DataType.valueOf(parts[1]), Integer.valueOf(parts[2]),
            parts[3], parts[4]));
      }
    }
    return list;
  }

  public byte[] getQualifier() {
    if (qualifier == null) qualifier = Bytes.toBytes(name);
    return qualifier;
  }

  public boolean isSet() {
    return isSet;
  }

  public String getName() {
    return name;
  }

  public byte[][] getKeys() {
    return isSet ? null : keys;
  }

  public long[] getCounts() {
    return isSet ? null : counts;
  }

  public Collection<Long> getSetValues() {
    return isSet ? setMap.values() : null;
  }

  public void updateRange(byte[] value) throws IOException {
    if (isSet) {
      if (!setMap.containsKey(value)) {
        throw new IOException("winter should be enum, but meet value out of enum");
      }
      long cur = setMap.get(value);
      setMap.put(value, cur + 1);
    } else {
      updateRangeArray(value);
    }
  }

  private void updateRangeArray(byte[] value) {
    int index = binarySearch(value);
    ++counts[index];
  }

  public int binarySearch(byte[] value) {
    int low = 0;
    int high = keys.length - 1;
    int middle = 0;
    while (low <= high) {
      middle = low + ((high - low) >> 1);
      int ret = LCCIndexConstant.compareValues(value, keys[middle], type);
      if (ret == 0) {
        return middle;
      } else if (ret < 0) {
        high = middle - 1;
      } else {
        low = middle + 1;
      }
    }
    return middle;
  }

  public int getSetKeyIndex(byte[] targetKey) {
    if (!isSet) return -1;
    int counter = 0;
    for (byte[] key : setMap.keySet()) {
      if (Bytes.equals(key, targetKey)) {
        return counter;
      }
      ++counter;
    }
    return -2;
  }
}
