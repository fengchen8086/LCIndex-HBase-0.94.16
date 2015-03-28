package org.apache.hadoop.hbase.ccindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Specification for index on one column.
 * @author wanhao
 */
public class IndexSpecification implements Writable {
  public static enum IndexType {
    /**
     * complementary clustering index
     */
    CCINDEX,
    /**
     * secondary index
     */
    SECONDARYINDEX,
    /**
     * improved secondary index(with additional columns)
     */
    IMPSECONDARYINDEX
  }

  // table name that contains this indexColumn
  private byte[] tableName = null;

  // family and qualifier of this indexColumn
  private byte[] family = null;
  private byte[] qualifier = null;

  // this index column
  private byte[] indexColumn = null;

  // Id of this index, part of index table name
  private byte[] indexId = null;

  private IndexType indexType = null;

  // split keys for index table of this index
  private byte[][] splitKeys = null;

  /**
   * Get split keys for index table.
   * @return
   */
  public byte[][] getSplitKeys() {
    return splitKeys;
  }

  /**
   * Set split keys for index table. The index table will be created with an initial set of empty
   * regions defined by the specified split keys.
   * @param splitKeys
   */
  public void setSplitKeys(byte[][] splitKeys) {
    this.splitKeys = splitKeys;
  }

  // Additional columns to be included in the index table,excepting for the indexColumn
  // For IMPSECONDARYINDEX only.
  // For CCINDEX and SECONDARYINDEX, this is null.
  private Map<byte[], Set<byte[]>> additionMap = null;

  public Map<byte[], Set<byte[]>> getAdditionMap() {
    if (this.indexType != IndexType.IMPSECONDARYINDEX || additionMap == null) {
      return null;
    }
    return Collections.unmodifiableMap(additionMap);
  }

  /**
   * Get addition map as string, used in ruby shell.
   * @return
   */
  public String getAdditionMapAsString() {
    StringBuilder sb = new StringBuilder();

    if (this.indexType == IndexType.IMPSECONDARYINDEX) {
      if (this.additionMap.size() == 0) {
        sb.append("null");
      } else {
        for (Map.Entry<byte[], Set<byte[]>> entry : this.additionMap.entrySet()) {
          sb.append("{family:" + Bytes.toString(entry.getKey()));
          if (entry.getValue() != null && entry.getValue().size() != 0) {
            sb.append(", columns:");
            int count = 0;
            for (byte[] b : entry.getValue()) {
              if (count == 0) {
                sb.append(Bytes.toString(b));
              } else {
                sb.append("," + Bytes.toString(b));
              }
              count++;
            }
          }
          sb.append("} ");
        }
      }
    }

    return sb.toString();
  }

  /**
   * Add an addition family for an index column with the type of IMPSECONDARYINDEX. This will
   * overall column in the same family added by {@link addAdditionColumn}
   * @param family
   */
  public void addAdditionFamily(byte[] family) {
    if (this.indexType == IndexType.IMPSECONDARYINDEX) {
      if (family == null) {
        throw new IllegalArgumentException("additional family is null");
      }
      this.additionMap.put(family, null);
    }
  }

  /**
   * Remove an addition family of an index column with the type of IMPSECONDARYINDEX.
   * @param family
   */
  public void removeAdditionFamily(byte[] family) {
    if (this.indexType == IndexType.IMPSECONDARYINDEX) {
      if (family == null) {
        throw new IllegalArgumentException("additional family is null");
      }
      if (this.additionMap.containsKey(family)) {
        this.additionMap.remove(family);
      }
    }
  }

  /**
   * Add an addition column for an index column with the type of IMPSECONDARYINDEX. This will
   * overall family of this column added by {@link addAdditionFamily}
   * @param family
   * @param qualifier
   */
  public void addAdditionColumn(byte[] family, byte[] qualifier) {
    if (this.indexType == IndexType.IMPSECONDARYINDEX) {
      if (family == null || qualifier == null) {
        throw new IllegalArgumentException("additional family/qualifier is null!");
      }

      if (Bytes.equals(this.indexColumn, Bytes.add(family, Bytes.toBytes(":"), qualifier))) {
        throw new IllegalArgumentException("additional column could not be index column!");
      }

      Set<byte[]> list = this.additionMap.get(family);
      if (list == null) {
        list = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      }
      if (list.contains(qualifier)) {
        list.remove(qualifier);
      }
      list.add(qualifier);
      this.additionMap.put(family, list);
    }
  }

  /**
   * Remove an addition column of an index column with the type of IMPSECONDARYINDEX.
   * @param family
   * @param qualifier
   */
  public void removeAdditionColumn(byte[] family, byte[] qualifier) {
    if (this.indexType == IndexType.IMPSECONDARYINDEX) {
      if (family == null || qualifier == null) {
        throw new IllegalArgumentException("additional family/qualifier is null!");
      }
      if (additionMap.containsKey(family)) {
        Set<byte[]> list = this.additionMap.get(family);
        if (list != null) {
          if (list.contains(qualifier)) {
            list.remove(qualifier);
          }
          if (list.size() == 0) {
            this.additionMap.remove(family);
          }
        }
      }
    }
  }

  public IndexType getIndexType() {
    return this.indexType;
  }

  /**
   * Set type for this index column.
   * @param type
   */
  public void setIndexType(IndexType type) {
    if (this.indexType != type) {
      this.indexType = type;
      if (type == IndexType.IMPSECONDARYINDEX) {
        this.additionMap = new TreeMap<byte[], Set<byte[]>>(Bytes.BYTES_COMPARATOR);
      } else {
        this.additionMap = null;
      }
    }
  }

  public byte[] getTableName() {
    return this.tableName;
  }

  /**
   * Set table name for this index column.
   * @param tableName
   */
  public void setTableName(byte[] tableName) {
    this.tableName = tableName;
  }

  public byte[] getFamily() {
    return this.family;
  }

  public byte[] getQualifier() {
    return this.qualifier;
  }

  public byte[] getIndexColumn() {
    return indexColumn;
  }

  /**
   * Set column for this index.
   * @param indexColumn
   */
  public void setIndexColumn(byte[] indexColumn) {
    this.indexColumn = indexColumn;
    byte[][] fq = KeyValue.parseColumn(this.indexColumn);
    this.family = fq[0];
    this.qualifier = fq[1];
    this.indexId = Bytes.add(this.family, Bytes.toBytes("_"), this.qualifier);
  }

  /**
   * Constructor for Writable, DO NOT USE!
   */
  public IndexSpecification() {

  }

  /**
   * Create an IndexSpecification for an indexColumn with default type (IndexType.SECONDARYINDEX)
   * @param tableName
   * @param indexColumn column to build index
   */
  public IndexSpecification(byte[] tableName, byte[] indexColumn) {
    this(tableName, indexColumn, IndexType.SECONDARYINDEX);
  }

  /**
   * Create an IndexSpecification for an indexColumn.
   * @param tableName
   * @param indexColumn column to build index
   * @param type index type
   */
  public IndexSpecification(byte[] tableName, byte[] indexColumn, IndexType type) {
    this.tableName = tableName;
    this.indexColumn = indexColumn;
    byte[][] fq = KeyValue.parseColumn(this.indexColumn);
    this.family = fq[0];
    this.qualifier = fq[1];
    this.indexId = Bytes.add(this.family, Bytes.toBytes("_"), this.qualifier);
    this.indexType = type;

    if (this.indexType == IndexType.IMPSECONDARYINDEX) {
      this.additionMap = new TreeMap<byte[], Set<byte[]>>(Bytes.BYTES_COMPARATOR);
    }
  }

  /**
   * Create an IndexSpecification for an indexColumn with default type (IndexType.SECONDARYINDEX)
   * and null table name.
   * @param indexColumn column to build index
   */
  public IndexSpecification(byte[] indexColumn) {
    this(null, indexColumn, IndexType.SECONDARYINDEX);
  }

  /**
   * Create an IndexSpecification for an indexColumn with null table name.
   * @param indexColumn column to build index
   * @param type index type
   */
  public IndexSpecification(byte[] indexColumn, IndexType type) {
    this(null, indexColumn, type);
  }

  public void readFields(DataInput in) throws IOException {
    if (in.readBoolean()) {
      this.tableName = Bytes.readByteArray(in);
    } else {
      this.tableName = null;
    }
    this.indexId = Bytes.readByteArray(in);
    this.indexColumn = Bytes.readByteArray(in);
    byte[][] fq = KeyValue.parseColumn(this.indexColumn);
    this.family = fq[0];
    this.qualifier = fq[1];

    byte[] type = Bytes.readByteArray(in);
    this.indexType = IndexType.valueOf(Bytes.toString(type));

    if (this.indexType == IndexType.IMPSECONDARYINDEX) {
      this.additionMap = new TreeMap<byte[], Set<byte[]>>(Bytes.BYTES_COMPARATOR);
      int size = in.readInt();
      if (size != 0) {
        for (int i = 0; i < size; i++) {
          byte[] family = Bytes.readByteArray(in);
          int setsize = in.readInt();
          if (setsize != 0) {
            Set<byte[]> list = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
            for (int j = 0; j < setsize; j++) {
              byte[] temp = Bytes.readByteArray(in);
              list.add(temp);
            }
            this.additionMap.put(family, list);
          } else {
            this.additionMap.put(family, null);
          }
        }
      }
    }
  }

  public void write(DataOutput out) throws IOException {
    if (this.tableName != null) {
      out.writeBoolean(true);
      Bytes.writeByteArray(out, this.tableName);
    } else {
      out.writeBoolean(false);
    }
    Bytes.writeByteArray(out, this.indexId);
    Bytes.writeByteArray(out, this.indexColumn);
    Bytes.writeByteArray(out, Bytes.toBytes(this.indexType.toString()));

    if (this.indexType == IndexType.IMPSECONDARYINDEX) {
      int size = this.additionMap.size();
      out.writeInt(size);
      if (size != 0) {
        for (Map.Entry<byte[], Set<byte[]>> entry : this.additionMap.entrySet()) {
          Bytes.writeByteArray(out, entry.getKey());
          int setsize = 0;
          if (entry.getValue() != null) {
            setsize = entry.getValue().size();
          }
          out.writeInt(setsize);
          if (setsize != 0) {
            for (byte[] b : entry.getValue()) {
              Bytes.writeByteArray(out, b);
            }
          }
        }
      }
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Table => ");
    sb.append(Bytes.toString(this.tableName));
    sb.append(" , IndexID => ");
    sb.append(Bytes.toString(this.indexId));
    sb.append(" , IndexedColumn => ");
    sb.append(Bytes.toString(this.indexColumn));
    sb.append(" , IndexType => ");

    sb.append(this.indexType.toString());

    if (this.indexType == IndexType.IMPSECONDARYINDEX) {
      sb.append(", AdditionColumn => ");
      if (this.additionMap.size() == 0) {
        sb.append("null");
      } else {
        for (Map.Entry<byte[], Set<byte[]>> entry : this.additionMap.entrySet()) {
          sb.append("{family=" + Bytes.toString(entry.getKey()));
          sb.append(", columns=");
          if (entry.getValue() == null || entry.getValue().size() == 0) {
            sb.append("null");
          } else {
            for (byte[] b : entry.getValue()) {
              sb.append(Bytes.toString(b) + " ");
            }
          }
          sb.append("}");
        }
      }
    }

    return sb.toString();
  }

  public boolean equals(Object arg0) {
    if (this == arg0) {
      return true;
    }
    if (arg0 == null) {
      return false;
    }
    if (!(arg0 instanceof IndexSpecification)) {
      return false;
    }

    IndexSpecification index = (IndexSpecification) arg0;

    if (Bytes.equals(index.indexId, this.indexId)
        && (this.tableName == index.tableName || Bytes.equals(this.tableName, index.tableName))
        && index.indexType == this.indexType) {
      if (index.indexType != IndexType.IMPSECONDARYINDEX) {
        return true;
      } else {
        return index.additionMap.equals(this.additionMap);
      }
    }

    return false;
  }

  /**
   * Get index table name for this indexColumn.
   * @return table name
   * @throws IllegalArgumentException table name is null
   */
  public byte[] getIndexTableName() {
    if (this.tableName == null) {
      throw new IllegalArgumentException("table name is null");
    }
    return Bytes.add(this.tableName, Bytes.toBytes("-"), this.indexId);
  }

  /**
   * Get index table name for this indexColumn.
   * @return table name
   */
  public String getIndexTableNameAsString() {
    return Bytes.toString(getIndexTableName());
  }

}
