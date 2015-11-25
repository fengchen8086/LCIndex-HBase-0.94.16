package doTest.ir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.client.IndexDescriptor;
import org.apache.hadoop.hbase.index.regionserver.QuickSort;
import org.apache.hadoop.hbase.util.Bytes;

import doWork.LCCIndexConstant;

public class GenerateIndexFromIR {

  private static Configuration conf = null;
  private static String tableName = LCCIndexConstant.TEST_IRINDEX_TABLE_NAME;
  // private static Map<byte[], ArrayList<byte[]>> indexMap = null;
  private static Map<byte[], Object> indexMap = null;
  protected long indexTS = HConstants.LATEST_TIMESTAMP;
  public static final String DELIMITER = "#";
  // public static final String DELIMITER = "#";
  public static final String CF_Qualifier_PREFIX = "iq_";
  public static final String CF_FAMILY_PREFIX = "if_";
  // byte[] indexFamily = Bytes.toBytes("index-f");

  static {
    conf = HBaseConfiguration.create();
    indexMap = new TreeMap<byte[], Object>(Bytes.BYTES_COMPARATOR);
    indexMap.put(Bytes.toBytes("A"), null);
    indexMap.put(Bytes.toBytes("B"), null);
    System.out.println(indexMap.containsKey(Bytes.toBytes("A")) + "&&&&&&&&&&&&&");
  }

  public static void main(String[] args) throws IOException {
    new GenerateIndexFromIR().work();
  }

  private void work() {
    ArrayList<KeyValue> list = new ArrayList<KeyValue>();
    try {
      int counter = 2;
      HTable table = new HTable(conf, tableName);
      Scan s = new Scan();
      ResultScanner ss = table.getScanner(s);
      for (Result r : ss) {
        if (counter-- <= 0) break;
        for (KeyValue kv : r.raw()) {
          list.add(kv);
        }
      }
      table.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("result size: " + list.size());
    for (KeyValue kv : list) {
      // System.out.println(toPrint(kv));
      if (isIndexColumn(kv)) {
        System.out.print(new String(kv.getRow()) + " ");
        System.out.print(new String(kv.getFamily()) + ":");
        System.out.print(new String(kv.getQualifier()) + " ");
        if ("A".equals(Bytes.toString(kv.getQualifier()))) {
          System.out.println(Bytes.toDouble(kv.getValue()));
        } else {
          System.out.println(Bytes.toInt(kv.getValue()));
        }
      }
    }

    ArrayList<KeyValue> resList = generateIndexedKVList(list);
    System.out.println("************************** result size: " + resList.size());
    KeyValue[] array = resList.toArray(new KeyValue[resList.size()]);
    Arrays.sort(array, KeyValue.COMPARATOR);
//    Arrays.sort(array, LCCIndexConstant.WINTER_KV_CMP);
    // QuickSort.sort(array, KVAC);
    // printResults(resList);
    for (KeyValue kv : array) {
      System.out.println(toPrint(kv));
    }
  }

  private void printResults(ArrayList<KeyValue> resList) {
    for (KeyValue kv : resList) {
      System.out.println(toPrint(kv));
    }
  }

  private static String toPrint(KeyValue kv) {
    String valueStr;
    valueStr = Bytes.toString(kv.getValue());
    return Bytes.toString(kv.getRow()) + "[" + Bytes.toString(kv.getFamily()) + ":"
        + Bytes.toString(kv.getQualifier()) + "]" + valueStr;
  }

  private boolean isIndexColumn(KeyValue kv) {
    return indexMap.containsKey(kv.getQualifier());
  }

  private byte[] getIndexKey(KeyValue kv) {
    String valueStr = null;
    if ("A".equals(Bytes.toString(kv.getQualifier()))) {
      valueStr = String.valueOf(Bytes.toDouble(kv.getValue()));
    } else if ("B".equals(Bytes.toString(kv.getQualifier()))) {
      valueStr = String.valueOf(Bytes.toInt(kv.getValue()));
    }
    return Bytes.toBytes(valueStr + DELIMITER + Bytes.toString(kv.getQualifier()) + DELIMITER
        + Bytes.toString(kv.getRow()));
  }

  private KeyValue generateKeyValueFromExistingKey(byte[] key, String indexedKey,
      byte[] rawQualifier) {
    String temp[] = indexedKey.split(DELIMITER);
    return new KeyValue(key, Bytes.toBytes(CF_FAMILY_PREFIX + Bytes.toString(rawQualifier)),
        Bytes.toBytes(temp[1]), indexTS, KeyValue.Type.Put, Bytes.toBytes(temp[0]));
  }

  private KeyValue generateLCCKeyValue(String targetKey, byte[] rawQualifier, byte[] value) {
    if ("A".equals(Bytes.toString(rawQualifier))) {
      value = Bytes.toBytes(String.valueOf(Bytes.toDouble(value)));
    } else if ("B".equals(Bytes.toString(rawQualifier))) {
      value = Bytes.toBytes(String.valueOf(Bytes.toInt(value)));
    }
    String temp[] = targetKey.split(DELIMITER);
    return new KeyValue(Bytes.toBytes(targetKey), Bytes.toBytes(CF_FAMILY_PREFIX + temp[1]),
        rawQualifier, indexTS, KeyValue.Type.Put, value);
  }

  private ArrayList<KeyValue> generateIndexedKVList(List<KeyValue> kvList) {
    // this maps of rowkeys. the value is a list of all possible new keys generated by index
    Map<String, ArrayList<String>> rowkeyMap = new HashMap<String, ArrayList<String>>();
    ArrayList<KeyValue> results = new ArrayList<KeyValue>();
    for (KeyValue kv : kvList) {
      if (rowkeyMap.containsKey(Bytes.toString(kv.getRow()))) {
        // already has the rowkey, first get the list
        ArrayList<String> listOfIndexedKeys = rowkeyMap.get(Bytes.toString(kv.getRow()));
        for (String indexedKeyStr : listOfIndexedKeys) {
          results.add(generateLCCKeyValue(indexedKeyStr, kv.getQualifier(), kv.getValue()));
          if (isIndexColumn(kv)) {
            results.add(generateKeyValueFromExistingKey(getIndexKey(kv), indexedKeyStr,
              kv.getQualifier()));
          }
        }
        if (isIndexColumn(kv)) { // add new KeyValue back
          listOfIndexedKeys.add(Bytes.toString(getIndexKey(kv)));
        }
      } else { // new added rowkey
        ArrayList<String> listOfIndexedKeys = new ArrayList<String>();
        listOfIndexedKeys.add(Bytes.toString(getIndexKey(kv)));
        rowkeyMap.put(Bytes.toString(kv.getRow()), listOfIndexedKeys);
      }
    }
    return results;
  }
}
