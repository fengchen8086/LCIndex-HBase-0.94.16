package org.apache.hadoop.hbase.ccindex;

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Simple index key Generator used most commonly. This is the default key generator.
 * @author wanhao
 */
public class SimpleIndexKeyGenerator implements IndexKeyGenerator {

  @Override
  public byte[] createIndexRowKey(final IndexSpecification indexSpec, final Put put) {
    List<KeyValue> values = put.get(indexSpec.getFamily(), indexSpec.getQualifier());
    if (values == null || values.size() == 0) {
      return null;
    }
    if (values.size() > 1) {
      throw new IllegalArgumentException(
          "Make sure that there is at most one value for an index column in a Put");
    }
    byte[] value = values.get(0).getValue();
    return createIndexRowKey(put.getRow(), value);
  }

  @Override
  public byte[] createIndexRowKey(final IndexSpecification indexSpec, final Result result) {
    KeyValue kv = result.getColumnLatest(indexSpec.getFamily(), indexSpec.getQualifier());
    if (kv == null) return null;
    byte[] value = kv.getValue();
    return createIndexRowKey(result.getRow(), value);
  }

  @Override
  public byte[] createIndexRowKey(final IndexSpecification indexSpec, final Delete delete) {
    List<KeyValue> values = delete.getFamilyMap().get(indexSpec.getFamily());

    byte[] value = null;
    for (KeyValue kv : values) {
      if (Bytes.compareTo(kv.getFamily(), indexSpec.getFamily()) == 0
          && Bytes.compareTo(kv.getQualifier(), indexSpec.getQualifier()) == 0) {
        value = kv.getValue();
        break;
      }
    }
    return createIndexRowKey(delete.getRow(), value);
  }

  @Override
  public byte[] createIndexRowKey(byte[] rowKey, byte[] value) {
    if (value == null || value.length == 0) {
      return null;
    }
    value = Bytes.add(value, IndexConstants.MIN_ROW_KEY);

    byte[] a =
        Bytes.toBytes(IndexConstants.LASTPART_ZERO.substring(0, IndexConstants.LASTPART_LENGTH
            - ("" + value.length).length())
            + value.length);

    return Bytes.add(value, rowKey, a);
  }

  @Override
  public byte[][] parseIndexRowKey(byte[] indexKey) {
    int length =
        Integer.valueOf(Bytes.toString(Bytes.tail(indexKey, IndexConstants.LASTPART_LENGTH)))
            .intValue();

    byte[][] result = new byte[2][];

    // get row key of main data table
    result[0] = new byte[indexKey.length - IndexConstants.LASTPART_LENGTH - length];
    for (int i = 0; i < result[0].length; i++) {
      result[0][i] = indexKey[i + length];
    }

    // get index column value
    result[1] = Bytes.head(indexKey, length - 1);

    return result;
  }

  @Override
  public String toString() {
    return "I am the default simple key generator!";
  }

}
