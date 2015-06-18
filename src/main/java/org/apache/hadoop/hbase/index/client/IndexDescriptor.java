package org.apache.hadoop.hbase.index.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.VersionedWritable;
import org.apache.hadoop.io.WritableUtils;

/**
 * The description of an indexed column family qualifier.
 * <p>
 * The description is composed of the following properties:
 * <ol>
 * <li>qualifier - specified which column to index. The values stored to this qualifier will serve
 * as index keys.
 * <li>type - type information for the qualifier. The type information allows for custom ordering of
 * index keys (which are qualifier values) which may come handy when range queries are executed.
 * <li>offset - combine this property with the length property to allow partial value extraction.
 * Useful for keeping the index size small while for qualifiers with large values. the offset
 * specifies the starting point in the value from which to extract the index key
 * <li>length - see also offset's description, the length property allows to limit the number of
 * bytes extracted to serve as index keys. If the bytes are random a length of 1 or 2 bytes would
 * yield very good results.
 * </ol>
 * </p>
 */
public class IndexDescriptor extends VersionedWritable {
  private static final byte VERSION = 1;

  private byte[] qualifier;

  /**
   * The qualifier type - affects the translation of bytes into indexed properties.
   */
  private DataType type = DataType.STRING;

  /**
   * Where to grab the column qualifier's value from. The default is from its first byte.
   */
  private int offset = 0;

  /**
   * Up-to where to grab the column qualifier's value. The default is all of it. A positive number
   * would indicate a set limit.
   */
  private int length = -1;

  /**
   * Empty constructor to support the writable interface - DO NOT USE.
   */
  public IndexDescriptor() {

  }

  /**
   * Construct a new index descriptor.
   * @param qualifier the qualifier name
   */
  public IndexDescriptor(byte[] qualifier) {
    this.qualifier = qualifier;
  }

  /**
   * Construct a new index descriptor.
   * @param qualifier the qualifier name
   * @param type the qualifier type
   */
  public IndexDescriptor(byte[] qualifier, DataType type) {
    this(qualifier);
    this.type = type;
  }

  /**
   * Construct a new index descriptor.
   * @param qualifier the qualifier name
   * @param type the qualifier type
   * @param offset the offset (from kv value start) from which to extract the index key
   * @param length the length to extract (everything by default)
   */
  public IndexDescriptor(byte[] qualifier, DataType type, int offset, int length) {
    this(qualifier, type);
    this.offset = offset;
    this.length = length;
  }

  /**
   * The column family qualifier name.
   * @return column family qualifier name
   */
  public byte[] getQualifier() {
    return qualifier;
  }

  /**
   * The column family qualifier name.
   * @param qualifier column family qualifier name
   */
  public void setQualifier(byte[] qualifier) {
    this.qualifier = qualifier;
  }

  /**
   * The data type that the column family qualifier contains.
   * @return data type that the column family qualifier contains
   */
  public DataType getType() {
    return type;
  }

  /**
   * The data type that the column family qualifier contains.
   * @param type data type that the column family qualifier contains
   */
  public void setType(DataType type) {
    this.type = type;
  }

  /**
   * The offset from which to extract the values.
   * @return the current offset value.
   */
  public int getOffset() {
    return offset;
  }

  /**
   * Sets the offset
   * @param offset the offset from which to extract the values.
   */
  public void setOffset(int offset) {
    this.offset = offset;
  }

  /**
   * The length of the block extracted from the qualifier's value.
   * @return the length of the extracted value
   */
  public int getLength() {
    return length;
  }

  /**
   * The length of the extracted value.
   * @param length the length of the extracted value.
   */
  public void setLength(int length) {
    this.length = length;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    super.write(dataOutput);
    Bytes.writeByteArray(dataOutput, qualifier);
    WritableUtils.writeEnum(dataOutput, type);
    dataOutput.writeInt(offset);
    dataOutput.writeInt(length);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    super.readFields(dataInput);
    qualifier = Bytes.readByteArray(dataInput);
    type = WritableUtils.readEnum(dataInput, DataType.class);
    this.offset = dataInput.readInt();
    this.length = dataInput.readInt();
  }

  @Override
  public byte getVersion() {
    return VERSION;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IndexDescriptor that = (IndexDescriptor) o;

    if (!Bytes.equals(qualifier, that.qualifier)) return false;

    if (this.type != that.type) return false;

    if (this.offset != that.offset) return false;

    if (this.length != that.length) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(qualifier);
  }

  @Override
  public String toString() {
    StringBuffer s = new StringBuffer();
    s.append("{ QUALIFIER => ");
    s.append(Bytes.toString(qualifier));
    s.append(", TYPE => ");
    s.append(type);
    s.append(", OFFSET => ");
    s.append(offset);
    s.append(", LENGTH => ");
    s.append(length);
    s.append(" }");
    return s.toString();
  }
}
