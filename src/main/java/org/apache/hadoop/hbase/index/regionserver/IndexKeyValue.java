package org.apache.hadoop.hbase.index.regionserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.RawComparator;

/**
 * <p>
 * IndexKeyValue wraps a byte array and has offset and length for passed array
 * at where to start interpreting the content as a IndexKeyValue blob. The
 * IndexKeyValue blob format inside the byte array is:
 * <code>&lt;key_length> &lt;value_length> &lt;key> &lt;value></code> Key is
 * decomposed as:
 * <code>&lt;qualifier_length> &lt;qualifier> &lt;qualifier_value></code>
 * qualifier and qualifier_value is IndexKeyValue's row.
 * 
 * Value is KeyValue's row key.
 * 
 */
public class IndexKeyValue {
  public static final int KEY_INFRASTRUCTURE_SIZE = Bytes.SIZEOF_INT /* qualifier_length */;

  // How far into the key the qualifier starts at. First thing to read is the
  // int
  // that says how long the qualifier is.
  public static final int QUALIFIER_OFFSET = Bytes.SIZEOF_INT /* key_length */
      + Bytes.SIZEOF_INT /* value_length */;

  // Size of the length ints in a IndexKeyValue data structure.
  public static final int KEYVALUE_INFRASTRUCTURE_SIZE = QUALIFIER_OFFSET;

  private byte[] bytes = null;
  private int offset = 0;
  private int length = 0;

  public IndexKeyValue() {

  }

  public IndexKeyValue(KeyValue kv) {
    this(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), kv.getBuffer(), kv
        .getQualifierOffset(), kv.getQualifierLength(), kv.getBuffer(), kv.getValueOffset(), kv
        .getValueLength());
  }

  public IndexKeyValue(final byte[] bytes) {
    this(bytes, 0);
  }

  public IndexKeyValue(final byte[] bytes, final int offset) {
    this(bytes, offset, getLength(bytes, offset));
  }

  public IndexKeyValue(final byte[] bytes, final int offset, final int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  public IndexKeyValue(byte[] row, byte[] qualifier, byte[] value) {
    this(row, 0, row == null ? 0 : row.length, qualifier, 0, qualifier == null ? 0
        : qualifier.length, value, 0, value == null ? 0 : value.length);
  }

  public IndexKeyValue(byte[] row, int roffset, int rlength, byte[] qualifier, int qoffset,
      int qlength, byte[] value, int voffset, int vlength) {
    this.bytes =
        createByteArray(row, roffset, rlength, qualifier, qoffset, qlength, value, voffset, vlength);
    this.length = bytes.length;
    this.offset = 0;
  }

  /**
   * Write KeyValue format into a byte array.
   * @param row
   * @param roffset
   * @param rlength
   * @param qualifier
   * @param qoffset
   * @param qlength
   * @param value
   * @param voffset
   * @param vlength
   * @return
   */
  static byte[] createByteArray(byte[] row, int roffset, int rlength, byte[] qualifier,
      int qoffset, int qlength, byte[] value, int voffset, int vlength) {
    rlength = row == null ? 0 : rlength;
    if (rlength > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Row > " + Short.MAX_VALUE);
    }

    // Qualifier length
    qlength = qualifier == null ? 0 : qlength;
    // Value length
    vlength = value == null ? 0 : vlength;
    if (qlength + vlength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Qualifier + Value > " + Integer.MAX_VALUE);
    }

    // Key length
    long longkeylength = KEY_INFRASTRUCTURE_SIZE + qlength + vlength;
    if (longkeylength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("keylength " + longkeylength + " > " + Integer.MAX_VALUE);
    }
    int keylength = (int) longkeylength;

    // Allocate right-sized byte array.
    byte[] bytes = new byte[KEYVALUE_INFRASTRUCTURE_SIZE + keylength + rlength];
    // write key and row length.
    int pos = 0;
    pos = Bytes.putInt(bytes, pos, keylength);
    pos = Bytes.putInt(bytes, pos, rlength);

    // write qualifier length
    pos = Bytes.putInt(bytes, pos, qlength);
    // write qualifier and qualifier value
    if (qlength != 0) {
      pos = Bytes.putBytes(bytes, pos, qualifier, qoffset, qlength);
    }
    if (vlength != 0) {
      pos = Bytes.putBytes(bytes, pos, value, voffset, vlength);
    }

    if (rlength != 0) {
      pos = Bytes.putBytes(bytes, pos, row, roffset, rlength);
    }
    return bytes;
  }

  // Needed doing 'contains' on List. Only compares the key portion, not the
  // value.
  public boolean equals(Object other) {
    if (!(other instanceof IndexKeyValue)) {
      return false;
    }
    IndexKeyValue kv = (IndexKeyValue) other;
    // Comparing bytes should be fine doing equals test. Shouldn't have to
    // worry about special .META. comparators doing straight equals.
    boolean result =
        Bytes.BYTES_RAWCOMPARATOR.compare(getBuffer(), getKeyOffset(), getKeyLength(),
          kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength()) == 0;
    return result;
  }

  public int hashCode() {
    byte[] b = getBuffer();
    int start = getOffset(), end = getOffset() + getLength();
    int h = b[start++];
    for (int i = start; i < end; i++) {
      h = (h * 13) ^ b[i];
    }
    return h;
  }

  // ---------------------------------------------------------------------------
  //
  // KeyValue cloning
  //
  // ---------------------------------------------------------------------------

  /**
   * Clones a KeyValue. This creates a copy, re-allocating the buffer.
   * @return Fully copied clone of this KeyValue
   */
  public KeyValue clone() {
    byte[] b = new byte[this.length];
    System.arraycopy(this.bytes, this.offset, b, 0, this.length);
    KeyValue ret = new KeyValue(b, 0, b.length);
    return ret;
  }

  // ---------------------------------------------------------------------------
  //
  // String representation
  //
  // ---------------------------------------------------------------------------

  public String toString() {
    if (this.bytes == null || this.bytes.length == 0) {
      return "empty";
    }
    int keyLength = Bytes.toInt(bytes, offset);
    int valueLength = Bytes.toInt(bytes, offset + Bytes.SIZEOF_INT);

    int off = offset + QUALIFIER_OFFSET;
    int qualifierLength = Bytes.toInt(bytes, off);
    off += Bytes.SIZEOF_INT;
    String qualifier = Bytes.toStringBinary(bytes, off, qualifierLength);
    off += qualifierLength;
    String qualifierValue =
        Bytes.toStringBinary(bytes, off, keyLength - Bytes.SIZEOF_INT - qualifierLength);
    String row =
        Bytes.toStringBinary(bytes, offset + KEYVALUE_INFRASTRUCTURE_SIZE + keyLength, valueLength);
    return qualifier + "/" + qualifierValue + "/" + row;
  }

  /**
   * @param k Key portion of a KeyValue.
   * @return Key as a String.
   */
  public static String keyToString(final byte[] k) {
    return keyToString(k, 0, k.length);
  }

  /**
   * Use for logging.
   * @param b Key portion of a KeyValue.
   * @param o Offset to start of key
   * @param l Length of key.
   * @return Key as a String.
   */
  public static String keyToString(final byte[] b, final int o, final int l) {
    if (b == null) return "";
    int qualifierlength = Bytes.toInt(b, o);
    String qualifier = Bytes.toStringBinary(b, o + Bytes.SIZEOF_INT, qualifierlength);
    String qualifierValue =
        Bytes.toStringBinary(b, o + Bytes.SIZEOF_INT + qualifierlength, l - Bytes.SIZEOF_INT
            - qualifierlength);

    return qualifier + "/" + qualifierValue;
  }

  // ---------------------------------------------------------------------------
  //
  // Public Member Accessors
  //
  // ---------------------------------------------------------------------------

  /**
   * @return The byte array backing this KeyValue.
   */
  public byte[] getBuffer() {
    return this.bytes;
  }

  /**
   * @return Offset into {@link #getBuffer()} at which this KeyValue starts.
   */
  public int getOffset() {
    return this.offset;
  }

  /**
   * @return Length of bytes this KeyValue occupies in {@link #getBuffer()}.
   */
  public int getLength() {
    return length;
  }

  // ---------------------------------------------------------------------------
  //
  // Length and Offset Calculators
  //
  // ---------------------------------------------------------------------------

  /**
   * Determines the total length of the KeyValue stored in the specified byte array and offset.
   * Includes all headers.
   * @param bytes byte array
   * @param offset offset to start of the KeyValue
   * @return length of entire KeyValue, in bytes
   */
  private static int getLength(byte[] bytes, int offset) {
    return KEYVALUE_INFRASTRUCTURE_SIZE + Bytes.toInt(bytes, offset)
        + Bytes.toInt(bytes, offset + Bytes.SIZEOF_INT);
  }

  /**
   * @return Key offset in backing buffer..
   */
  public int getKeyOffset() {
    return this.offset + QUALIFIER_OFFSET;
  }

  public String getKeyString() {
    return Bytes.toStringBinary(getBuffer(), getKeyOffset(), getKeyLength());
  }

  public int getKeyLength() {
    return Bytes.toInt(this.bytes, this.offset);
  }

  /**
   * @return Value offset
   */
  public int getValueOffset() {
    return getKeyOffset() + getKeyLength();
  }

  /**
   * @return Value length
   */
  public int getValueLength() {
    return Bytes.toInt(this.bytes, this.offset + Bytes.SIZEOF_INT);
  }

  /**
   * @return Row offset
   */
  public int getRowOffset() {
    return getKeyOffset() + getKeyLength();
  }

  /**
   * @return Row length
   */
  public int getRowLength() {
    return Bytes.toInt(this.bytes, this.offset + Bytes.SIZEOF_INT);
  }

  /**
   * @return Qualifier offset
   */
  public int getQualifierOffset() {
    return this.offset + QUALIFIER_OFFSET + Bytes.SIZEOF_INT;
  }

  /**
   * @return Qualifier length
   */
  public int getQualifierLength() {
    return Bytes.toInt(bytes, offset + QUALIFIER_OFFSET);
  }

  /**
   * @return Qualifier value offset
   */
  public int getQualifierValueOffset() {
    return getQualifierOffset() + getQualifierLength();
  }

  /**
   * @return Qualifier value length
   */
  public int getQualifierValueLength() {
    return getKeyLength() - getQualifierLength() - Bytes.SIZEOF_INT;
  }

  // ---------------------------------------------------------------------------
  //
  // Methods that return copies of fields
  //
  // ---------------------------------------------------------------------------

  /**
   * Do not use unless you have to. Used internally for compacting and testing.
   * @return Copy of the key portion only.
   */
  public byte[] getKey() {
    int keylength = getKeyLength();
    byte[] key = new byte[keylength];
    System.arraycopy(getBuffer(), getKeyOffset(), key, 0, keylength);
    return key;
  }

  /**
   * Returns value in a new byte array.
   * @return Value in a new byte array.
   */
  public byte[] getValue() {
    int o = getValueOffset();
    int l = getValueLength();
    byte[] result = new byte[l];
    System.arraycopy(getBuffer(), o, result, 0, l);
    return result;
  }

  /**
   * Returns the row of this IndexKeyValue in a new byte array.
   * @return Row in a new byte array.
   */
  public byte[] getRow() {
    int o = getRowOffset();
    int l = getRowLength();
    byte[] result = new byte[l];
    System.arraycopy(getBuffer(), o, result, 0, l);
    return result;
  }

  /**
   * Returns qualifier in a new byte array.
   * @return qualifier
   */
  public byte[] getQualifier() {
    int o = getQualifierOffset();
    int l = getQualifierLength();
    byte[] result = new byte[l];
    System.arraycopy(getBuffer(), o, result, 0, l);
    return result;
  }

  /**
   * Returns qualifier value in a new byte array.
   * @return qualifier value
   */
  public byte[] getQualifierValue() {
    int o = getQualifierValueOffset();
    int l = getQualifierValueLength();
    byte[] result = new byte[l];
    System.arraycopy(getBuffer(), o, result, 0, l);
    return result;
  }

  // HeapSize
  public long heapSize() {
    return ClassSize.align(ClassSize.OBJECT + (4 * ClassSize.REFERENCE)
        + ClassSize.align(ClassSize.ARRAY) + ClassSize.align(length) + (2 * Bytes.SIZEOF_INT)
        + Bytes.SIZEOF_LONG);
  }

  // this overload assumes that the length bytes have already been read,
  // and it expects the length of the KeyValue to be explicitly passed
  // to it.
  public void readFields(int length, final DataInput in) throws IOException {
    this.length = length;
    this.offset = 0;
    this.bytes = new byte[this.length];
    in.readFully(this.bytes, 0, this.length);
  }

  // Writable
  public void readFields(final DataInput in) throws IOException {
    int length = in.readInt();
    readFields(length, in);
  }

  public void write(final DataOutput out) throws IOException {
    out.writeInt(this.length);
    out.write(this.bytes, this.offset, this.length);
  }

  // ---------------------------------------------------------------------------
  //
  // Compare specified fields against those contained in this IndexKeyValue
  //
  // ---------------------------------------------------------------------------

  /**
   * @param qualifierValue
   * @return True if matching qualifier value.
   */
  public boolean matchingQualifierValue(final byte[] qualifierValue) {
    return matchingQualifierValue(qualifierValue, 0, qualifierValue == null ? 0
        : qualifierValue.length);
  }

  public boolean matchingQualifierValue(final byte[] qualifierValue, int offset, int length) {
    return Bytes.compareTo(qualifierValue, offset, length, this.bytes,
      this.getQualifierValueOffset(), this.getQualifierValueLength()) == 0;
  }

  public boolean matchingQualifierValue(final IndexKeyValue other) {
    return matchingQualifierValue(other.getBuffer(), other.getQualifierValueOffset(),
      other.getQualifierValueLength());
  }

  /**
   * @param qualifier
   * @return True if matching qualifiers.
   */
  public boolean matchingQualifier(final byte[] qualifier) {
    return matchingQualifier(qualifier, 0, qualifier.length);
  }

  public boolean matchingQualifier(final byte[] qualifier, int offset, int length) {
    return Bytes.compareTo(qualifier, offset, length, this.bytes, getQualifierOffset(),
      getQualifierLength()) == 0;
  }

  public boolean matchingQualifier(final IndexKeyValue other) {
    return matchingQualifier(other.getBuffer(), other.getQualifierOffset(),
      other.getQualifierLength());
  }

  public boolean matchingRow(final byte[] row) {
    return matchingRow(row, 0, row == null ? 0 : row.length);
  }

  public boolean matchingRow(final byte[] row, int offset, int length) {
    return Bytes.compareTo(row, offset, length, this.bytes, getRowOffset(), getRowLength()) == 0;
  }

  public boolean matchingRow(IndexKeyValue other) {
    return matchingRow(other.getBuffer(), other.getRowOffset(), other.getRowLength());
  }

  /**
   * @param b
   * @return A IndexKeyValue made of a byte array that holds the key-only part. Needed to convert
   *         IndexFile index members to KeyValues.
   */
  public static IndexKeyValue createIndexKeyValueFromKey(final byte[] b) {
    return createIndexKeyValueFromKey(b, 0, b.length);
  }

  /**
   * @param bb
   * @return A IndexKeyValue made of a byte array that holds the key-only part. Needed to convert
   *         IndexFile index members to KeyValues.
   */
  public static IndexKeyValue createIndexKeyValueFromKey(final ByteBuffer bb) {
    return createIndexKeyValueFromKey(bb.array(), bb.arrayOffset(), bb.limit());
  }

  /**
   * @param b
   * @param o
   * @param l
   * @return A IndexKeyValue made of a byte array that holds the key-only part. Needed to convert
   *         IndexFile index members to KeyValues.
   */
  public static IndexKeyValue createIndexKeyValueFromKey(final byte[] b, final int o, final int l) {
    byte[] newb = new byte[b.length + QUALIFIER_OFFSET];
    System.arraycopy(b, o, newb, QUALIFIER_OFFSET, l);
    Bytes.putInt(newb, 0, b.length);
    Bytes.putInt(newb, Bytes.SIZEOF_INT, 0);
    return new IndexKeyValue(newb);
  }

  /**
   * Create a IndexKeyValue for the specified qualifier that would be smaller than all other
   * possible IndexKeyValues that have the same qualifier. Used for seeking.
   * @param qualifier - column qualifier
   * @return First possible key on passed qualifier
   */
  public static IndexKeyValue createFirstOnQualifier(final byte[] qualifier) {
    return new IndexKeyValue(null, qualifier, null);
  }

  /**
   * Create a IndexKeyValue for the specified qualifier and value that would be smaller than all
   * other possible IndexKeyValues that have the same qualifier and value. Used for seeking.
   * @param qualifier - column qualifier
   * @param value - qualifier value
   * @return First possible key on passed qualifier and value
   */
  public static IndexKeyValue createFirstOnQualifier(final byte[] qualifier, final byte[] value) {
    return new IndexKeyValue(null, qualifier, value);
  }

  /**
   * Create a IndexKeyValue for the specified qualifier and value that would be smaller than all
   * other possible IndexKeyValues that have the same qualifier and value. Used for seeking.
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param value column qualifier value
   * @param voffset value offset
   * @param vlength value length
   * @return First possible key on passed qualifier and value
   */
  public static IndexKeyValue createFirstOnQualifier(final byte[] qualifier, final int qoffset,
      final int qlength, final byte[] value, final int voffset, final int vlength) {
    return new IndexKeyValue(null, 0, 0, qualifier, qoffset, qlength, value, voffset, vlength);
  }

  /**
   * Create a IndexKeyValue for the specified qualifier that would be larger than all other possible
   * IndexKeyValues that have the same qualifier.
   * @param qualifier column qualifier
   * @param value qualifier value
   * @return Last possible key on passed qualifier.
   */
  public static IndexKeyValue createLastOnQualifier(final byte[] qualifier, final byte[] value) {
    return new IndexKeyValue(null, qualifier, Bytes.add(value, new byte[] { 0x00 }));
  }

  /**
   * Create a IndexKeyValue for the specified qualifier that would be larger than all other possible
   * IndexKeyValues that have the same qualifier.
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param value column qualifier value
   * @param voffset value offset
   * @param vlength value length
   * @return Last possible key on passed qualifier.
   */
  public static IndexKeyValue createLastOnQualifier(final byte[] qualifier, final int qoffset,
      final int qlength, final byte[] value, final int voffset, final int vlength) {
    return new IndexKeyValue(null, 0, 0, qualifier, qoffset, qlength, Bytes.add(value,
      new byte[] { 0x00 }), voffset, vlength + 1);
  }

  /**
   * Compare key portion of a {@link IndexKeyValue}.
   */
  public static class IndexKeyComparator implements RawComparator<byte[]> {
    public int
        compare(byte[] left, int loffset, int llength, byte[] right, int roffset, int rlength) {
      // compare qualifier
      int lqOffset = Bytes.SIZEOF_INT + loffset;
      int rqOffset = Bytes.SIZEOF_INT + roffset;
      int lqLength = Bytes.toInt(left, loffset);
      int rqLength = Bytes.toInt(right, roffset);

      int compare = Bytes.compareTo(left, lqOffset, lqLength, right, rqOffset, rqLength);
      if (compare != 0) {
        return compare;
      }

      // compare qualifier value
      int lqvOffset = lqOffset + lqLength;
      int rqvOffset = rqOffset + rqLength;
      int lqvLength = llength - lqLength - Bytes.SIZEOF_INT;
      int rqvLength = rlength - rqLength - Bytes.SIZEOF_INT;

      compare = Bytes.compareTo(left, lqvOffset, lqvLength, right, rqvOffset, rqvLength);
      return compare;
    }

    public int compare(byte[] left, byte[] right) {
      return compare(left, 0, left.length, right, 0, right.length);
    }

    public int compareQualifier(byte[] left, int loffset, int llength, byte[] right, int roffset,
        int rlength) {
      return Bytes.compareTo(left, loffset, llength, right, roffset, rlength);
    }

  }

  /**
   * Compare IndexKeyValues. When we compare IndexKeyValues, we only compare the Key portion. This
   * means two IndexKeyValues with same Key but different Values are considered the same as far as
   * this Comparator is concerned. Hosts a {@link IndexKeyComparator}.
   */
  public static class IndexKVComparator implements java.util.Comparator<IndexKeyValue> {
    private final IndexKeyComparator rawcomparator = new IndexKeyComparator();

    public IndexKeyComparator getRawComparator() {
      return this.rawcomparator;
    }

    public int compare(final IndexKeyValue left, final IndexKeyValue right) {
      int ret =
          Bytes.compareTo(left.getBuffer(), left.getQualifierOffset(), left.getQualifierLength(),
            right.getBuffer(), right.getQualifierOffset(), right.getQualifierLength());
      if (ret != 0) {
        return ret;
      }

      ret =
          Bytes.compareTo(left.getBuffer(), left.getQualifierValueOffset(),
            left.getQualifierValueLength(), right.getBuffer(), right.getQualifierValueOffset(),
            right.getQualifierValueLength());

      if (ret != 0) {
        return ret;
      }

      ret =
          Bytes.compareTo(left.getBuffer(), left.getValueOffset(), left.getValueLength(),
            right.getBuffer(), right.getValueOffset(), right.getValueLength());
      return ret;
    }

    public int compareKey(final IndexKeyValue left, final IndexKeyValue right) {
      return getRawComparator().compare(left.getBuffer(), left.getKeyOffset(), left.getKeyLength(),
        right.getBuffer(), right.getKeyOffset(), right.getKeyLength());
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
      return new IndexKVComparator();
    }

  }

  /**
   * Comparator for plain key/values; i.e. non-catalog table key/values.
   */
  public static IndexKVComparator COMPARATOR = new IndexKVComparator();
}
