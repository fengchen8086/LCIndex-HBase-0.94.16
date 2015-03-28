package org.apache.hadoop.hbase.ccindex;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ccindex.IndexSpecification.IndexType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * IndexDescriptor holds index information of a table.
 * <p>
 * There may be several types of index in a table, including CCindex, SecondaryIndex and Improved
 * SecondaryIndex. Each index is defined in an {@link IndexSpecification} instance.
 * <p>
 * An index is defined on a column, and each column has no more than one index.
 * @author wanhao
 */
public class IndexTableDescriptor {
  // base table descriptor of this index descriptor
  private final HTableDescriptor descriptor;

  // all IndexSpecifications
  private final Map<byte[], IndexSpecification> indexSpecifications =
      new TreeMap<byte[], IndexSpecification>(Bytes.BYTES_COMPARATOR);

  // all index columns, constructed from all IndexSpecifications
  private byte[][] indexedColumns;

  // all families which contain index columns, constructed from all IndexSpecifications
  private Set<byte[]> indexedFamiliesSet;

  public Set<byte[]> getIndexedFamiliesSet() {
    return indexedFamiliesSet;
  }

  public byte[][] getIndexedColumns() {
    return indexedColumns;
  }

  // split keys for main data table, they will be used only where you create table
  private byte[][] splitKeys = null;

  /**
   * Get split keys for main data table.
   * @return
   */
  public byte[][] getSplitKeys() {
    return splitKeys;
  }

  /**
   * Set split keys for main data table. The main data table will be created with an initial set of
   * empty regions defined by the specified split keys.
   * @param splitKeys
   */
  public void setSplitKeys(byte[][] splitKeys) {
    this.splitKeys = splitKeys;
  }

  private Class<? extends IndexKeyGenerator> keygen = SimpleIndexKeyGenerator.class;
  private IndexKeyGenerator keygenerator = null;

  /**
   * Set key generator class name.
   * @param className
   * @throws ClassNotFoundException
   */
  public void setKeygenClass(String className) throws ClassNotFoundException {
    Class<?> tempkeygen = null;
    if (className.contains(".")) {
      tempkeygen = Class.forName(className);
    } else {
      tempkeygen = Class.forName(this.getClass().getPackage().getName() + "." + className);
    }
    if (!IndexKeyGenerator.class.isAssignableFrom(tempkeygen)) {
      throw new IllegalArgumentException(tempkeygen.getName()
          + "doesn't implement interface IndexKeyGenerator!");
    }
    keygen = tempkeygen.asSubclass(IndexKeyGenerator.class);
    descriptor.setValue(IndexConstants.KEYGEN, Bytes.toBytes(keygen.getName()));
  }

  public Class<? extends IndexKeyGenerator> getKeygenClass() {
    return keygen;
  }

  /**
   * Get index key generator instance.
   * @return
   * @throws IOException-if get instance from class failed
   */
  public IndexKeyGenerator getKeyGenerator() throws IOException {
    if (keygenerator == null) {
      try {
        Constructor<?> cons = keygen.getConstructor();
        keygenerator = (IndexKeyGenerator) cons.newInstance();
      } catch (Exception e) {
        throw new IOException(e.getMessage());
      }
    }
    return keygenerator;
  }

  public IndexSpecification[] getIndexSpecifications() {
    return indexSpecifications.values().toArray(new IndexSpecification[0]);
  }

  public IndexSpecification getIndexSpecification(byte[] indexColumn)
      throws IndexNotExistedException {
    if (!this.indexSpecifications.containsKey(indexColumn)) {
      throw new IndexNotExistedException(Bytes.toString(indexColumn));
    }
    return indexSpecifications.get(indexColumn);
  }

  private void checkNewIndex(IndexSpecification indexSpec) {
    indexSpec.setTableName(this.descriptor.getName());
    if (indexSpec.getIndexType() == IndexType.IMPSECONDARYINDEX) {
      if (indexSpec.getAdditionMap().size() != 0) {
        Set<byte[]> families = this.descriptor.getFamiliesKeys();
        for (byte[] temp : indexSpec.getAdditionMap().keySet()) {
          if (!families.contains(temp)) {
            throw new IllegalArgumentException("Error addition family " + Bytes.toString(temp)
                + " for index column " + Bytes.toString(indexSpec.getIndexColumn()));
          }
        }
      }
    }
  }

  /**
   * Add an IndexSpecification to table descriptor. This table should not be existed, otherwise you
   * don't actually add an index to table, please use {@code IndexAdmin#addIndex} to add an index to
   * table in actually. If IndexSpecification's TableName is not equal to this tableName, it will be
   * set to this tableName automatically.
   * @param index
   * @throws IOException
   * @throws IndexExistedException
   */
  public void addIndex(IndexSpecification index) throws IOException, IndexExistedException {
    if (index == null) {
      throw new IllegalArgumentException("IndexSpecification is null");
    }

    if (this.indexSpecifications.containsKey(index.getIndexColumn())) {
      throw new IndexExistedException(Bytes.toString(index.getIndexColumn()));
    }

    checkNewIndex(index);

    indexSpecifications.put(index.getIndexColumn(), index);
    this.update();
    this.writeToTable();
  }

  /**
   * Add IndexSpecifications to table descriptor. This table should not be existed, otherwise you
   * don't actually add an index to table, please use {@code IndexAdmin#addIndexes} to add an
   * indexes to table in actually. If any IndexSpecification's TableName is not equal to this
   * tableName, it will be set to this tableName automatically.
   * @param indexes
   * @throws IOException
   * @throws IndexExistedException
   */
  public void addIndexes(IndexSpecification[] indexes) throws IOException, IndexExistedException {
    if (indexes == null || indexes.length == 0) {
      throw new IllegalArgumentException("IndexSpecification array is null or empty");
    }
    for (IndexSpecification indexSpec : indexes) {
      if (this.indexSpecifications.containsKey(indexSpec.getIndexColumn())) {
        throw new IndexExistedException(Bytes.toString(indexSpec.getIndexColumn()));
      }
      checkNewIndex(indexSpec);
      indexSpecifications.put(indexSpec.getIndexColumn(), indexSpec);
    }
    this.update();
    this.writeToTable();
  }

  /**
   * Delete an Index from table descriptor. This table should not be existed, otherwise you don't
   * actually delete an index from table, please use {@code IndexAdmin#deleteIndex} to delete an
   * index from table in actually.
   * @param indexColumn
   * @throws IOException
   * @throws IndexNotExistedException
   */
  public void deleteIndex(byte[] indexColumn) throws IOException, IndexNotExistedException {
    if (indexColumn == null) {
      throw new IllegalArgumentException("Index column is null");
    }
    if (this.indexSpecifications.containsKey(indexColumn) == false) {
      throw new IndexNotExistedException(Bytes.toString(indexColumn));
    }
    indexSpecifications.remove(indexColumn);
    this.update();
    this.writeToTable();
  }

  /**
   * Delete Indexes from table descriptor. This table should not be existed, otherwise you don't
   * actually delete indexes from table, please use {@code IndexAdmin#deleteIndexes} to delete
   * indexes from table in actually.
   * @param indexColumns
   * @throws IOException
   * @throws IndexNotExistedException
   */
  public void deleteIndexes(byte[][] indexColumns) throws IOException, IndexNotExistedException {
    if (!hasIndex()) {
      throw new IllegalArgumentException("Index column array is null or empty");
    }
    for (byte[] column : indexColumns) {
      if (this.indexSpecifications.containsKey(column) == false) {
        throw new IndexNotExistedException(Bytes.toString(column));
      }
      indexSpecifications.remove(column);
    }
    this.update();
    this.writeToTable();
  }

  /**
   * Delete all IndexSpecifications.
   * @throws IOException
   */
  public void deleteAllIndexes() throws IOException {
    this.indexSpecifications.clear();
    this.update();
    this.writeToTable();
  }

  /**
   * Update indexColumns, indexedFamiliesSet and IndexSpcification.indexColumns after modify
   * indexSpcifications.
   */
  private void update() {
    int size = this.indexSpecifications.keySet().size();
    this.indexedColumns = new byte[size][];
    this.indexedFamiliesSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

    int i = 0;
    for (IndexSpecification indexSpec : this.indexSpecifications.values()) {
      this.indexedColumns[i++] = indexSpec.getIndexColumn();
      byte[][] fq = KeyValue.parseColumn(indexSpec.getIndexColumn());
      this.indexedFamiliesSet.add(fq[0]);
    }

    for (IndexSpecification indexSpec : this.indexSpecifications.values()) {
      indexSpec.setTableName(this.descriptor.getName());
    }
  }

  /**
   * Construct from HTableDescriptor, table can be existed or not. If the table corresponding to
   * tableDescriptor has indexes, then read these indexes automatically, else you must add some
   * indexes in case you want a table with indexes. If you want to add indexes to table descriptor,
   * you can call addIndexSpecification() or addIndexSpecifications().
   * @param tableDescriptor
   * @throws IOException
   */
  public IndexTableDescriptor(HTableDescriptor tableDescriptor) throws IOException {
    this.descriptor = tableDescriptor;

    if (Bytes.compareTo(descriptor.getName(), HConstants.META_TABLE_NAME) == 0
        || Bytes.compareTo(descriptor.getName(), HConstants.ROOT_TABLE_NAME) == 0) {
      throw new IllegalArgumentException("Table name can't be "
          + Bytes.toString(HConstants.META_TABLE_NAME) + " or " + HConstants.ROOT_TABLE_NAME);
    }

    this.readFromTable();
  }

  /**
   * Construct from HTableDescriptor and add some indexes to table descriptor at the same time.
   * Table must be not existed, otherwise you don't actually add indexes to table, please use
   * {@code IndexAdmin#addIndexes} to add indexes to table in actually..
   * @param tableDescriptor
   * @param specifications
   * @throws IOException
   * @throws IndexExistedException
   */
  public IndexTableDescriptor(HTableDescriptor tableDescriptor, IndexSpecification[] specifications)
      throws IOException, IndexExistedException {
    this(tableDescriptor);
    this.addIndexes(specifications);
  }

  /**
   * Read IndexDescription from existed base table Description.
   * @throws IOException
   */
  private void readFromTable() throws IOException {
    byte[] bytes = descriptor.getValue(IndexConstants.INDEX_KEY);
    if (bytes == null) {
      return;
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);

    IndexSpecificationArray indexArray = new IndexSpecificationArray();
    indexArray.readFields(dis);

    for (IndexSpecification indexSpec : indexArray.getIndexSpecifications()) {
      checkNewIndex(indexSpec);
      indexSpecifications.put(indexSpec.getIndexColumn(), indexSpec);
    }
    this.update();

    byte[] kg = descriptor.getValue(IndexConstants.KEYGEN);

    if (kg != null && kg.length != 0) {
      try {
        Class<?> tempkeygen = Class.forName(Bytes.toString(kg));
        if (!IndexKeyGenerator.class.isAssignableFrom(tempkeygen)) {
          throw new IllegalArgumentException(tempkeygen.getName()
              + "doesn't implement interface IndexKeyGenerator!");
        }
        keygen = tempkeygen.asSubclass(IndexKeyGenerator.class);
      } catch (ClassNotFoundException e) {
        throw new IOException(e.getMessage());
      }
    }
  }

  /**
   * Write IndexDescription to base table Description.
   * @throws IOException
   */
  private void writeToTable() throws IOException {
    if (!this.indexSpecifications.isEmpty()) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);

      IndexSpecificationArray indexArray =
          new IndexSpecificationArray(indexSpecifications.values().toArray(
            new IndexSpecification[0]));

      indexArray.write(dos);
      dos.flush();

      descriptor.setValue(IndexConstants.INDEX_KEY, baos.toByteArray());
      descriptor.setValue(IndexConstants.BASE_KEY, IndexConstants.BASE_KEY);
      descriptor.setValue(IndexConstants.KEYGEN, Bytes.toBytes(keygen.getName()));
    } else {
      if (descriptor.getValue(IndexConstants.INDEX_KEY) != null) {
        descriptor.remove(IndexConstants.INDEX_KEY);
      }
      if (descriptor.getValue(IndexConstants.BASE_KEY) != null) {
        descriptor.remove(IndexConstants.BASE_KEY);
      }
      if (descriptor.getValue(IndexConstants.KEYGEN) != null) {
        descriptor.remove(IndexConstants.KEYGEN);
      }
    }
  }

  public HTableDescriptor getTableDescriptor() {
    return this.descriptor;
  }

  protected HTableDescriptor createIndexTableDescriptor(byte[] indexColumn)
      throws IndexNotExistedException {
    IndexSpecification indexSpec = this.getIndexSpecification(indexColumn);
    HTableDescriptor indexTableDescriptor = new HTableDescriptor(indexSpec.getIndexTableName());

    if (indexSpec.getIndexType() == IndexType.CCINDEX) {
      for (HColumnDescriptor desc : this.descriptor.getFamilies()) {
        indexTableDescriptor.addFamily(desc);
      }
    } else if (indexSpec.getIndexType() == IndexType.IMPSECONDARYINDEX) {
      Set<byte[]> family = indexSpec.getAdditionMap().keySet();
      if (family.size() != 0) {
        for (byte[] name : family) {
          indexTableDescriptor.addFamily(this.descriptor.getFamily(name));
        }
      } else {
        indexTableDescriptor.addFamily(this.descriptor.getFamily(indexSpec.getFamily()));
      }
    } else if (indexSpec.getIndexType() == IndexType.SECONDARYINDEX) {
      indexTableDescriptor.addFamily(this.descriptor.getFamily(indexSpec.getFamily()));
    }

    indexTableDescriptor.setValue(IndexConstants.INDEX_TYPE,
      Bytes.toBytes(indexSpec.getIndexType().toString())); // record the index type
    return indexTableDescriptor;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("{ ");
    sb.append("TableName => " + this.descriptor.getNameAsString() + " ,");
    sb.append("IndexColumns => ");
    if (this.indexedColumns != null && this.indexedColumns.length != 0) {
      for (byte[] column : this.indexedColumns) {
        sb.append(Bytes.toString(column) + " ");
      }
    }
    sb.append(" ,");

    sb.append("Families => ");
    if (this.indexedFamiliesSet != null && this.indexedFamiliesSet.size() != 0) {
      for (byte[] tmp : this.indexedFamiliesSet) {
        sb.append(Bytes.toString(tmp) + " ");
      }
    }

    sb.append(",\n");
    sb.append("IndexSpecifications => \n");
    if (this.indexSpecifications != null && this.indexSpecifications.size() != 0) {
      for (IndexSpecification index : this.indexSpecifications.values()) {
        sb.append(index.toString() + "\n");
      }
    }
    sb.append("}");
    return sb.toString();
  }

  public boolean equals(Object args) {
    if (this == args) {
      return true;
    }
    if (args == null) {
      return false;
    }
    if (!(args instanceof IndexTableDescriptor)) {
      return false;
    }

    IndexTableDescriptor outDesc = (IndexTableDescriptor) args;
    if (this.descriptor.equals(outDesc.descriptor)
        && this.indexSpecifications.equals(outDesc.indexSpecifications)) {
      return true;
    }

    return false;
  }

  /**
   * Check if table descriptor contains any index.
   * @return true if has index
   */
  public boolean hasIndex() {
    return (indexedColumns != null && indexedColumns.length != 0);
  }
}
