package org.apache.hadoop.hbase.index.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

/**
 * IndexTableDescriptor holds additional index information of a table.
 * <p>
 * An index is defined on a column, and each column has no more than one index.
 */
public class IndexColumnDescriptor extends HColumnDescriptor {
  public static final byte[] INDEX = Bytes.toBytes("INDEX");
  public static final byte[] INDEX_TYPE = Bytes.toBytes("INDEX_TYPE");
  // public static final byte[]

  // all IndexDescriptors
  public final Map<byte[], IndexDescriptor> indexes = new TreeMap<byte[], IndexDescriptor>(
      Bytes.BYTES_COMPARATOR);

  public enum IndexType {
    IRIndex, LCCIndex, CCIndex, Unknown;
  }

  public IndexType indexType = null;

  public IndexColumnDescriptor() {

  }

  public IndexColumnDescriptor(byte[] familyName) {
    super(familyName);
  }

  public IndexColumnDescriptor(String familyName) {
    // super(familyName);
    this(familyName, 1);
  }

  public IndexColumnDescriptor(String familyName, int typeValue) {
    super(familyName);
    indexType = typeOfIndexValue(typeValue);
    if (indexType == IndexType.Unknown) {
      System.out.println("winter typeValue int->IndexValue unknown, value: " + typeValue);
    }
  }

  public IndexColumnDescriptor(HColumnDescriptor desc) {
    super(desc);
    if (desc instanceof IndexColumnDescriptor) {
      System.out.println("winter add new IndexColumnDescriptor because of instanceof");
      IndexColumnDescriptor indexDesc = (IndexColumnDescriptor) desc;
      addIndexes(indexDesc.getAllIndex());
    } else {
      // System.out.println("winter operate table: " + desc.getNameAsString());
      byte[] bytes = desc.getValue(INDEX);

      if (bytes != null && bytes.length != 0) {
        DataInputBuffer indexin = new DataInputBuffer();
        indexin.reset(bytes, bytes.length);

        int size;
        try {
          indexType = typeOfIndexValue(indexin.readInt());
          size = indexin.readInt();
          for (int i = 0; i < size; i++) {
            IndexDescriptor indexDescriptor = new IndexDescriptor();
            indexDescriptor.readFields(indexin);
            indexes.put(indexDescriptor.getQualifier(), indexDescriptor);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public Map<byte[], IndexDescriptor> getAllIndexMap() {
    return Collections.unmodifiableMap(indexes);
  }

  public IndexDescriptor[] getAllIndex() {
    return indexes.values().toArray(new IndexDescriptor[0]);
  }

  public IndexDescriptor getIndex(byte[] qualifier) {
    if (!indexes.containsKey(qualifier)) {
      return null;
    }
    return indexes.get(qualifier);
  }

  /**
   * Add an IndexDescriptor to table descriptor.
   * @param index
   */
  public void addIndex(IndexDescriptor index) {
    if (index != null && index.getQualifier() != null) {
      indexes.put(index.getQualifier(), index);
    }
  }

  /**
   * Add IndexDescriptors to table descriptor.
   * @param indexes
   */
  public void addIndexes(IndexDescriptor[] indexes) {
    if (indexes != null && indexes.length != 0) {
      for (IndexDescriptor index : indexes) {
        addIndex(index);
      }
    }
  }

  /**
   * Delete an Index from table descriptor.
   * @param family
   */
  public void deleteIndex(byte[] qualifier) {
    if (indexes.containsKey(qualifier)) {
      indexes.remove(qualifier);
    }
  }

  /**
   * Delete all IndexSpecifications.
   * @throws IOException
   */
  public void deleteAllIndex() throws IOException {
    indexes.clear();
  }

  /**
   * Check if table descriptor contains any index.
   * @return true if has index
   */
  public boolean hasIndex() {
    return !indexes.isEmpty();
  }

  public IndexType getIndexType() {
    return indexType;
  }

  /**
   * Check if table descriptor contains the specific index.
   * @return true if has index
   */
  public boolean containIndex(byte[] qualifier) {
    return indexes.containsKey(qualifier);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    DataInputBuffer indexin = new DataInputBuffer();
    byte[] bytes = super.getValue(INDEX);
    indexin.reset(bytes, bytes.length);

    int size = indexin.readInt();
    for (int i = 0; i < size; i++) {
      IndexDescriptor indexDescriptor = new IndexDescriptor();
      indexDescriptor.readFields(indexin);
      indexes.put(indexDescriptor.getQualifier(), indexDescriptor);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    DataOutputBuffer indexout = new DataOutputBuffer();
    System.out.println("winter write indexColumn descripter, indexType is: "
        + valueOfIndexType(indexType));
    indexout.writeInt(valueOfIndexType(indexType));
    indexout.writeInt(indexes.size());
    for (IndexDescriptor indexDescriptor : indexes.values()) {
      indexDescriptor.write(indexout);
    }
    super.setValue(INDEX, indexout.getData());
    super.write(out);
  }

  private static int valueOfIndexType(IndexType t) {
    if (t == IndexType.IRIndex) return 1;
    if (t == IndexType.LCCIndex) return 2;
    if (t == IndexType.CCIndex) return 3;
    System.out.println("winter in IndexType->int unknown");
    return 0;
  }

  private static IndexType typeOfIndexValue(int i) {
    if (i == 1) return IndexType.IRIndex;
    if (i == 2) return IndexType.LCCIndex;
    if (i == 3) return IndexType.CCIndex;
    return IndexType.Unknown;
  }

  public TreeMap<byte[], DataType> mWinterGetQualifierType() {
    TreeMap<byte[], DataType> lccIndexQualifier =
        new TreeMap<byte[], DataType>(Bytes.BYTES_COMPARATOR);
    for (IndexDescriptor ids : getAllIndex()) {
      lccIndexQualifier.put(ids.getQualifier(), ids.getType());
    }
    return lccIndexQualifier;
  }
}
