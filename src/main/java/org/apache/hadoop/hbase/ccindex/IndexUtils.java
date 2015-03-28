package org.apache.hadoop.hbase.ccindex;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ccindex.IndexSpecification.IndexType;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;

/**
 * Singleton class for index maintence logic.
 * @author wanhao
 */
public class IndexUtils {

  public static byte[] DELIMITER = new byte[] { '#', '#', '#', '#' };

  public static IndexPut createIndexPut(final IndexTableDescriptor indexDesc, final Put put)
      throws IOException {
    IndexPut indexput = new IndexPut();
    indexput.addPut(IndexConstants.KEY, put);
    if (indexDesc.hasIndex()) {
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        byte[] indexRowKey = indexDesc.getKeyGenerator().createIndexRowKey(indexSpec, put);
        if (indexRowKey == null) continue;
        Put tempput = new Put(indexRowKey);
        if (indexSpec.getIndexType() == IndexType.CCINDEX) {
          for (Map.Entry<byte[], List<KeyValue>> entry : put.getFamilyMap().entrySet()) {
            if (Bytes.compareTo(entry.getKey(), indexSpec.getFamily()) == 0) {
              for (KeyValue kv : entry.getValue()) {
                if (Bytes.compareTo(kv.getQualifier(), indexSpec.getQualifier()) != 0) {
                  tempput.add(kv.getFamily(), kv.getQualifier(), kv.getTimestamp(), kv.getValue());
                } else if (put.size() == 1) { // contain index column only
                  tempput.add(kv.getFamily(), null, kv.getTimestamp(), kv.getValue());
                }
              }
            } else {
              for (KeyValue kv : entry.getValue()) {
                tempput.add(kv.getFamily(), kv.getQualifier(), kv.getTimestamp(), kv.getValue());
              }
            }
          }
        } else if (indexSpec.getIndexType() == IndexType.IMPSECONDARYINDEX) {
          Map<byte[], Set<byte[]>> addmap = indexSpec.getAdditionMap();
          for (Map.Entry<byte[], List<KeyValue>> entry : put.getFamilyMap().entrySet()) {
            if (addmap.containsKey(entry.getKey())) {
              Set<byte[]> colset = addmap.get(entry.getKey());
              // family which index column belongs to
              if (Bytes.compareTo(indexSpec.getFamily(), entry.getKey()) == 0) {
                // addition family
                if (colset == null || colset.size() == 0) {
                  for (KeyValue kv : entry.getValue()) {
                    if (Bytes.compareTo(kv.getQualifier(), indexSpec.getQualifier()) != 0) {
                      System.out.println("winter A");
                      tempput.add(kv.getFamily(), kv.getQualifier(), kv.getTimestamp(),
                        kv.getValue());
                    } else if (put.size() == 1) { // contain index column only
                      System.out.println("winter B");
                      tempput.add(kv.getFamily(), null, kv.getTimestamp(), kv.getValue());
                    }
                  }
                } else {
                  for (KeyValue kv : entry.getValue()) {
                    if (colset.contains(kv.getQualifier())) {
                      if (Bytes.compareTo(kv.getQualifier(), indexSpec.getQualifier()) != 0) {
                        System.out.println("winter C");
                        tempput.add(kv.getFamily(), kv.getQualifier(), kv.getTimestamp(),
                          kv.getValue());
                      } else if (put.size() == 1) { // contain index column only
                        System.out.println("winter D");
                        tempput.add(kv.getFamily(), null, kv.getTimestamp(), kv.getValue());
                      }
                    }
                  }
                }
              } else {
                // addition family
                if (colset == null || colset.size() == 0) {
                  for (KeyValue kv : entry.getValue()) {
                    tempput
                        .add(kv.getFamily(), kv.getQualifier(), kv.getTimestamp(), kv.getValue());
                  }
                } else {
                  for (KeyValue kv : entry.getValue()) {
                    if (colset.contains(kv.getQualifier())) {
                      tempput.add(kv.getFamily(), kv.getQualifier(), kv.getTimestamp(),
                        kv.getValue());
                    }
                  }
                }
              }
            }
          }
        } else if (indexSpec.getIndexType() == IndexType.SECONDARYINDEX) {
          // need to do nothing!
          tempput.add(indexSpec.getFamily(), null, null);
        } else {
          tempput.add(indexSpec.getFamily(), null, null);
        }
        indexput.addPut(indexSpec.getIndexColumn(), tempput);
      }
    }
    return indexput;
  }

  public static IndexDelete createIndexDelete(IndexTableDescriptor indexDesc, Delete delete,
      Result result) throws IOException {
    IndexDelete indexdelete = new IndexDelete();
    indexdelete.addDelete(IndexConstants.KEY, delete);
    if (result != null && !result.isEmpty()) {
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        byte[] rowkey = indexDesc.getKeyGenerator().createIndexRowKey(indexSpec, result);
        if (rowkey != null) {
          Delete temp = new Delete(rowkey);
          boolean delWholeRow = false;
          if (delete.isEmpty()) {
            delWholeRow = true;
          } else {
            if (delete.getFamilyMap().containsKey(indexSpec.getFamily())) {
              for (KeyValue kv : delete.getFamilyMap().get(indexSpec.getFamily())) {
                if (kv.getQualifierLength() == 0) {
                  if (Bytes.compareTo(kv.getFamily(), indexSpec.getFamily()) == 0) {
                    delWholeRow = true;
                    break;
                  }
                } else {
                  if (Bytes.compareTo(kv.getFamily(), indexSpec.getFamily()) == 0
                      && Bytes.compareTo(kv.getQualifier(), indexSpec.getQualifier()) == 0) {
                    delWholeRow = true;
                    break;
                  }
                }
              }
            }
          }
          if (!delWholeRow) {
            if (indexSpec.getIndexType() == IndexType.CCINDEX) {
              for (Map.Entry<byte[], List<KeyValue>> entry : delete.getFamilyMap().entrySet()) {
                for (KeyValue kv : entry.getValue()) {
                  if (kv.getQualifierLength() == 0) {
                    temp.deleteFamily(kv.getFamily());
                  } else {
                    temp.deleteColumn(kv.getFamily(), kv.getQualifier());
                  }
                }
              }
            } else if (indexSpec.getIndexType() == IndexType.IMPSECONDARYINDEX) {
              for (Map.Entry<byte[], List<KeyValue>> entry : delete.getFamilyMap().entrySet()) {
                for (KeyValue kv : entry.getValue()) {
                  if (kv.getQualifierLength() == 0) {
                    if (indexSpec.getAdditionMap().containsKey(kv.getFamily())) {
                      temp.deleteFamily(kv.getFamily());
                    }
                  } else {
                    if (indexSpec.getAdditionMap().containsKey(kv.getFamily())) {
                      Set<byte[]> qua = indexSpec.getAdditionMap().get(kv.getFamily());
                      if (qua == null || qua.contains(kv.getQualifier())) {
                        temp.deleteColumn(kv.getFamily(), kv.getQualifier());
                      }
                    }
                  }
                }
              }
            }
          }
          indexdelete.addDelete(indexSpec.getIndexColumn(), temp);
        }
      }
    }
    return indexdelete;
  }

}
