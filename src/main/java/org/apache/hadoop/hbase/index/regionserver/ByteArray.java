package org.apache.hadoop.hbase.index.regionserver;

import java.util.Comparator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class ByteArray {
  private byte[] bytes = null;
  private int offset = 0;
  private int length = 0;
  private int hash = -1;

  public ByteArray(final byte[] bytes, final int offset, final int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  public byte[] getByteArray() {
    byte[] result = new byte[length];
    System.arraycopy(bytes, offset, result, 0, length);
    return result;
  }

  public int hashCode() {
    if (hash < 0) {
      hash = 0;
      int off = offset;
      for (int i = 0; i < length; i++) {
        hash = 31 * hash + bytes[off++];
      }
    }
    return hash;
  }

  public boolean equals(Object other) {
    if (!(other instanceof ByteArray)) {
      return false;
    }
    ByteArray ba = (ByteArray) other;
    boolean result = Bytes.compareTo(bytes, offset, length, ba.bytes, ba.offset, ba.length) == 0;
    return result;
  }

  public static Comparator<ByteArray> BAC = new ByteArrayComparator();
  
  static class ByteArrayComparator implements Comparator<ByteArray> {

    @Override
    public int compare(ByteArray o1, ByteArray o2) {
      return Bytes.compareTo(o1.bytes, o1.offset, o1.length, o2.bytes, o2.offset, o2.length);
    }
  };
}
