package org.apache.hadoop.hbase.ccindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ccindex.IndexSpecification.IndexType;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * IndexAdmin maintains indexes of tables, such as create index table, add indexes and delete
 * indexes.
 * @author wanhao
 */
public class CCIndexAdmin {
  private static final Log LOG = LogFactory.getLog(CCIndexAdmin.class);
  private HBaseAdmin admin = null;
  private boolean isTest = false;

  private Configuration conf = null;

  public Configuration getConfiguration() {
    return this.conf;
  }

  public HBaseAdmin getHBaseAdmin() {
    return this.admin;
  }

  /**
   * Construct an IndexAdmin with default Configuration.
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   */
  public CCIndexAdmin() throws MasterNotRunningException, ZooKeeperConnectionException {
    this.conf = HBaseConfiguration.create();
    this.admin = new HBaseAdmin(this.conf);
  }

  /**
   * Construct an IndexAdmin with the given Configuration.
   * @param conf Configuration object
   * @throws MasterNotRunningException
   * @throws ZooKeeperConnectionException
   */
  public CCIndexAdmin(Configuration conf) throws MasterNotRunningException,
      ZooKeeperConnectionException {
    this.conf = conf;
    this.admin = new HBaseAdmin(this.conf);
  }

  /**
   * Construct an IndexAdmin with the given HBaseAdmin.
   * @param admin
   */
  public CCIndexAdmin(HBaseAdmin admin) {
    this.conf = admin.getConfiguration();
    this.admin = admin;
  }

  /**
   * Creates a new table.
   * @param desc table descriptor for table
   * @throws IOException if a remote or network exception occurs
   */
  public void createTable(HTableDescriptor desc) throws IOException {
    admin.createTable(desc);
  }

  /**
   * Creates a new table with indexes defined by IndexDescriptor.
   * @param indexDesc table descriptor for table
   * @throws IOException
   * @throws IndexExistedException
   */
  public void createTable(IndexTableDescriptor indexDesc) throws IOException, IndexExistedException {
    HTableDescriptor descriptor = new HTableDescriptor(indexDesc.getTableDescriptor());
    descriptor.remove(IndexConstants.INDEX_KEY);
    admin.createTable(descriptor, indexDesc.getSplitKeys());
    admin.disableTable(descriptor.getName());

    if (indexDesc.hasIndex()) {
      this.addIndexes(indexDesc.getTableDescriptor().getName(), indexDesc.getIndexSpecifications());
    }
    enableTable(descriptor.getName());
  }

  /**
   * Delete a table and all of its indexes, if existed.
   * @param tableName name of table to delete
   * @throws IOException-if a remote or network exception occurs
   */
  public void deleteTable(String tableName) throws IOException {
    deleteTable(Bytes.toBytes(tableName));
  }

  /**
   * Delete a table and all of its indexes, if existed.
   * @param tableName name of table to delete
   * @throws IOException-if a remote or network exception occurs
   */
  public void deleteTable(byte[] tableName) throws IOException {
    if (isTableEnabled(tableName)) {
      throw new IOException("Table " + Bytes.toString(tableName) + " is enabled! Disable it first!");
    }

    IndexTableDescriptor indexDesc = new IndexTableDescriptor(admin.getTableDescriptor(tableName));

    if (indexDesc.hasIndex()) {
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (admin.tableExists(indexSpec.getIndexTableName())) {
          admin.deleteTable(indexSpec.getIndexTableName());
        }
      }
    }
    admin.deleteTable(tableName);
  }

  /**
   * Check if table is existed.
   * @param tableName name of table to check
   * @return true if table is existed
   * @throws IOException-if a remote or network exception occurs
   */
  public boolean tableExists(String tableName) throws IOException {
    return tableExists(Bytes.toBytes(tableName));
  }

  /**
   * Check if table is existed.
   * @param tableName name of table to check
   * @return true if table is existed
   * @throws IOException-if a remote or network exception occurs
   */
  public boolean tableExists(byte[] tableName) throws IOException {
    try {
      HTableDescriptor desc = admin.getTableDescriptor(tableName);
      if (isIndexTable(desc)) {
        return false;
      }
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /**
   * Enable a table, if existed.
   * @param tableName name of table to enable
   * @throws IOException-if a remote or network exception occurs
   */
  public void enableTable(String tableName) throws IOException {
    enableTable(Bytes.toBytes(tableName));
  }

  /**
   * Enable a table, if existed.
   * @param tableName name of table to enable
   * @throws IOException-if a remote or network exception occurs
   */
  public void enableTable(byte[] tableName) throws IOException {
    HTableDescriptor desc = admin.getTableDescriptor(tableName);
    if (isIndexTable(desc)) {
      throw new TableNotFoundException(Bytes.toString(tableName));
    }
    IndexTableDescriptor indexDesc = new IndexTableDescriptor(desc);

    if (indexDesc.hasIndex()) {
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (admin.tableExists(indexSpec.getIndexTableName())) {
          admin.enableTable(indexSpec.getIndexTableName());
        } else {
          throw new IndexMissingException(tableName, indexSpec);
        }
      }
    }
    admin.enableTable(tableName);
  }

  /**
   * Disable a table, if existed.
   * @param tableName name of table to disable
   * @throws IOException-if a remote or network exception occurs
   */
  public void disableTable(String tableName) throws IOException {
    disableTable(Bytes.toBytes(tableName));
  }

  /**
   * Disable a table, if existed.
   * @param tableName name of table to disable
   * @throws IOException-if a remote or network exception occurs
   */
  public void disableTable(byte[] tableName) throws IOException {
    HTableDescriptor desc = admin.getTableDescriptor(tableName);
    if (isIndexTable(desc)) {
      throw new TableNotFoundException(Bytes.toString(tableName));
    }
    IndexTableDescriptor indexDesc = new IndexTableDescriptor(desc);

    if (indexDesc.hasIndex()) {
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (admin.tableExists(indexSpec.getIndexTableName())) {
          admin.disableTable(indexSpec.getIndexTableName());
        } else {
          throw new IndexMissingException(tableName, indexSpec);
        }
      }
    }
    admin.disableTable(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if table is enabled
   * @throws IOException-if a remote or network exception occurs
   */
  public boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if table is enabled
   * @throws IOException-if a remote or network exception occurs
   */
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    HTableDescriptor desc = admin.getTableDescriptor(tableName);
    if (isIndexTable(desc)) {
      throw new TableNotFoundException(Bytes.toString(tableName));
    }
    IndexTableDescriptor indexDesc = new IndexTableDescriptor(desc);

    boolean isenable = admin.isTableEnabled(tableName);

    if (indexDesc.hasIndex()) {
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (admin.tableExists(indexSpec.getIndexTableName())) {
          if (isenable && admin.isTableDisabled(indexSpec.getIndexTableName())) {
            admin.enableTable(indexSpec.getIndexTableName());
          } else if (!isenable && admin.isTableEnabled(indexSpec.getIndexTableName())) {
            admin.disableTable(indexSpec.getIndexTableName());
          }
        } else {
          throw new IndexMissingException(tableName, indexSpec);
        }
      }
    }

    return isenable;
  }

  /**
   * @param tableName name of table to check
   * @return true if table is disabled
   * @throws IOException-if a remote or network exception occurs
   */
  public boolean isTableDisabled(String tableName) throws IOException {
    return !isTableEnabled(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if table is disabled
   * @throws IOException-if a remote or network exception occurs
   */
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return !isTableEnabled(tableName);
  }

  /**
   * @param tableName name of table to check
   * @return true if table and indexes are all available
   * @throws IOException-if a remote or network exception occurs
   */
  public boolean isTableAvailable(String tableName) throws IOException {
    return isTableAvailable(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if table and indexes are all available
   * @throws IOException-if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    HTableDescriptor desc = admin.getTableDescriptor(tableName);
    if (isIndexTable(desc)) {
      throw new TableNotFoundException(Bytes.toString(tableName));
    }
    IndexTableDescriptor indexDesc = new IndexTableDescriptor(desc);

    if (indexDesc.hasIndex()) {
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (admin.tableExists(indexSpec.getIndexTableName())) {
          if (!admin.isTableAvailable(indexSpec.getIndexTableName())) {
            return false;
          }
        } else {
          throw new IndexMissingException(tableName, indexSpec);
        }
      }
    }

    if (!admin.isTableAvailable(tableName)) {
      return false;
    }
    return true;
  }

  /**
   * list all tables, including tables with or without indexes.
   * @return an array of {@link HTableDescriptor}
   * @throws IOException
   */
  public HTableDescriptor[] listTables() throws IOException {
    ArrayList<HTableDescriptor> descList = new ArrayList<HTableDescriptor>();
    HTableDescriptor[] tableDescriptor = admin.listTables();

    if (tableDescriptor != null && tableDescriptor.length != 0) {
      for (HTableDescriptor desc : tableDescriptor) {
        byte[] indexType = desc.getValue(IndexConstants.INDEX_TYPE);
        // table without any index or main data table
        if (indexType == null) {
          descList.add(desc);
        }
      }
    }

    return descList.toArray(new HTableDescriptor[0]);
  }

  /**
   * Check if it is an index table.
   * @param desc
   * @throws IllegalArgumentException-desc is null
   */
  private boolean isIndexTable(HTableDescriptor desc) throws IOException {
    if (desc == null) {
      throw new IllegalArgumentException("Table Descriptor is empty");
    }
    byte[] value = desc.getValue(IndexConstants.INDEX_TYPE);

    return (value != null) ? true : false;
  }

  /**
   * Check if a table has indexes.
   * @param tableName
   * @return true if table has any index
   * @throws IOException
   */
  public boolean hasIndex(String tableName) throws IOException {
    return hasIndex(Bytes.toBytes(tableName));
  }

  /**
   * Check if a table has indexes.
   * @param tableName
   * @return true if table has any index
   * @throws IOException
   */
  public boolean hasIndex(byte[] tableName) throws IOException {
    HTableDescriptor desc = admin.getTableDescriptor(tableName);
    if (isIndexTable(desc)) {
      throw new TableNotFoundException(Bytes.toString(tableName));
    }
    IndexTableDescriptor indexDesc = new IndexTableDescriptor(desc);
    return indexDesc.hasIndex();
  }

  /**
   * List all index columns of table.
   * @param tableName
   * @return
   * @throws IOException
   */
  public byte[][] listIndex(String tableName) throws IOException {
    return listIndex(Bytes.toBytes(tableName));
  }

  /**
   * List all index columns of table.
   * @param tableName
   * @return
   * @throws IOException
   */
  public byte[][] listIndex(byte[] tableName) throws IOException {
    HTableDescriptor desc = admin.getTableDescriptor(tableName);
    if (isIndexTable(desc)) {
      throw new TableNotFoundException(Bytes.toString(tableName));
    }
    IndexTableDescriptor indexDesc = new IndexTableDescriptor(desc);
    return indexDesc.getIndexedColumns();
  }

  /**
   * Get IndexDescriptor of a table.
   * @param tableName
   * @return IndexDescriptor of the table
   * @throws IOException
   */
  public IndexTableDescriptor getIndexTableDescriptor(byte[] tableName) throws IOException {
    HTableDescriptor desc = admin.getTableDescriptor(tableName);
    if (isIndexTable(desc)) {
      throw new TableNotFoundException(Bytes.toString(tableName));
    }
    return new IndexTableDescriptor(desc);
  }

  /**
   * Add an index to the table.
   * @param tableName
   * @param indexSpec
   * @throws IOException
   * @throws IndexExistedException
   */
  public void addIndex(byte[] tableName, IndexSpecification indexSpec) throws IOException,
      IndexExistedException {
    if (indexSpec == null) {
      throw new IllegalArgumentException("Invalid index specfication!");
    }
    this.addIndexes(tableName, new IndexSpecification[] { indexSpec });
  }

  /**
   * Add indexes to the table.
   * @param tableName
   * @param indexSpecs if any IndexSpecification's TableName is not equal to tableName, it will be
   *          set to tableName.
   * @throws IOException
   * @throws IndexExistedException
   */
  public void addIndexes(byte[] tableName, IndexSpecification[] indexSpecs) throws IOException,
      IndexExistedException {
    if (indexSpecs == null || indexSpecs.length == 0) {
      throw new IllegalArgumentException("Invalid index specfication array!");
    }
    if (isTableEnabled(tableName)) {
      throw new IOException("Table " + Bytes.toString(tableName) + " is enabled! Disable it first!");
    }

    IndexTableDescriptor indexDesc = new IndexTableDescriptor(admin.getTableDescriptor(tableName));
    indexDesc.addIndexes(indexSpecs);
    // backup, used in case of exceptions
    HTableDescriptor backupdesc = admin.getTableDescriptor(tableName);
    try {
      // create tables of indexes which want to be added
      for (IndexSpecification spec : indexSpecs) {
        admin.createTable(indexDesc.createIndexTableDescriptor(spec.getIndexColumn()),
          spec.getSplitKeys());
        admin.disableTable(spec.getIndexTableName());
      }
      // modify base table
      admin.modifyTable(tableName, indexDesc.getTableDescriptor());
      admin.enableTable(tableName);
      admin.disableTable(tableName);
    } catch (IOException e) {
      LOG.warn("Add indexes failed! Try to undo!", e);
      try {
        for (IndexSpecification spec1 : indexSpecs) {
          if (admin.tableExists(spec1.getIndexTableName())) {
            admin.disableTable(spec1.getIndexTableName());
            admin.deleteTable(spec1.getIndexTableName());
          }
        }

        // modify base table
        admin.modifyTable(tableName, backupdesc);
        admin.enableTable(tableName);
        admin.disableTable(tableName);
        LOG.info("Try to undo add indexes successfully!");
      } catch (IOException e1) {
        LOG.error("Try to undo add indexes failed!", e1);
        throw e1;
      }
    } catch (IndexNotExistedException e) {
      LOG.error("An impossible exception occurs!", e);
    }

    if (isTest) {
      return;
    }

  }

  /**
   * Delete an Index.
   * @param tableName
   * @param indexedColumn
   * @throws IOException
   * @throws IndexNotExistedException
   */
  public void deleteIndex(byte[] tableName, byte[] indexedColumn) throws IOException,
      IndexNotExistedException {
    if (indexedColumn == null || indexedColumn.length == 0) {
      throw new IllegalArgumentException("Invalid index column!");
    }
    this.deleteIndexes(tableName, new byte[][] { indexedColumn });
  }

  /**
   * Delete Indexes.
   * @param tableName
   * @param indexedColumns
   * @throws IOException
   * @throws IndexNoExistedException
   */
  public void deleteIndexes(byte[] tableName, byte[][] indexedColumns) throws IOException,
      IndexNotExistedException {
    if (indexedColumns == null || indexedColumns.length == 0) {
      throw new IllegalArgumentException("Invalid index columns!");
    }

    if (isTableEnabled(tableName)) {
      throw new IOException("Table " + Bytes.toString(tableName) + " is enabled! Disable it first!");
    }

    IndexTableDescriptor indexDesc = new IndexTableDescriptor(admin.getTableDescriptor(tableName));

    // get IndexSpecifications of indexes which want to be deleted
    IndexSpecification[] deletedIndexSpec = new IndexSpecification[indexedColumns.length];
    for (int i = 0; i < indexedColumns.length; i++) {
      deletedIndexSpec[i] = indexDesc.getIndexSpecification(indexedColumns[i]);
    }

    indexDesc.deleteIndexes(indexedColumns);

    try {
      // delete tables
      for (IndexSpecification spec : deletedIndexSpec) {
        if (admin.isTableEnabled(spec.getIndexTableName())) {
          admin.disableTable(spec.getIndexTableName());
        }
        admin.deleteTable(spec.getIndexTableName());
      }

      // modify base table
      admin.modifyTable(tableName, indexDesc.getTableDescriptor());
      admin.enableTable(tableName);
      admin.disableTable(tableName);
    } catch (Exception e) {
      LOG.warn("Delete indexes failed! Try again!", e);
      try {
        // delete tables
        for (IndexSpecification spec : deletedIndexSpec) {
          if (admin.tableExists(spec.getIndexTableName())) {
            admin.disableTable(spec.getIndexTableName());
            admin.deleteTable(spec.getIndexTableName());
          }
        }
        // modify base table
        admin.modifyTable(tableName, indexDesc.getTableDescriptor());
        admin.enableTable(tableName);
        admin.disableTable(tableName);
        LOG.info("Try to delete indexes again successfully!");
      } catch (IOException e1) {
        LOG.error("Try to delete indexes again failed!", e1);
        throw e1;
      }
    }
  }

  /**
   * Fix up an index's missing data in the table.
   * <p>
   * This table is not needed to be disabled before calling this method, and after the return the
   * table will be enabled.
   * @param tableName
   * @throws IOException
   */
  public void fixupMissingIndexes(String tableName) throws IOException {
    fixupMissingIndexes(Bytes.toBytes(tableName));
  }

  /**
   * Fix up an index's missing data in the table.
   * <p>
   * This table is not needed to be disabled before calling this method, and after the return the
   * table will be enabled.
   * @param tableName
   * @throws IOException
   */
  public void fixupMissingIndexes(byte[] tableName) throws IOException {
    IndexTableDescriptor indexDesc = new IndexTableDescriptor(admin.getTableDescriptor(tableName));

    if (!indexDesc.hasIndex()) {
      return;
    }

    try {
      admin.disableTable(tableName);
      String tmp = null;

      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (admin.tableExists(indexSpec.getIndexTableName())) {
          admin.disableTable(indexSpec.getIndexTableName());
        } else {
          try {
            admin.createTable(indexDesc.createIndexTableDescriptor(indexSpec.getIndexColumn()),
              indexSpec.getSplitKeys());
          } catch (IndexNotExistedException e) {
            // impossible here
          }
          admin.disableTable(indexSpec.getIndexTableName());
          if (tmp == null) {
            tmp = Bytes.toString(indexSpec.getIndexColumn());
          } else {
            tmp += "," + Bytes.toString(indexSpec.getIndexColumn());
          }
        }
      }
      if (tmp == null) {
        return;
      }
      if (isTest) {
        return;
      }

    } catch (IOException e3) {
      throw new IOException("Rebuild index failed, cause message:" + e3.getMessage(), e3);
    }

    String errStr = "";
    for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
      if (admin.tableExists(indexSpec.getIndexTableName())) {
        admin.enableTable(indexSpec.getIndexTableName());
      } else {
        errStr +=
            "[column:" + Bytes.toString(indexSpec.getIndexColumn()) + ",type:"
                + indexSpec.getIndexType().toString() + "]";
      }
    }
    admin.enableTable(tableName);
    if (!errStr.isEmpty()) {
      throw new IOException("Try to fix up missing index " + errStr
          + " failed, maybe you have to clear up it(them)!");
    }

  }

  /**
   * Clear up an index's record in the table if its index data is missing.
   * <p>
   * This table is not needed to be disabled before calling this method, and after the return the
   * table will be enabled.
   * @param tableName
   * @throws IOException
   */
  public void clearupMissingIndexes(String tableName) throws IOException {
    clearupMissingIndexes(Bytes.toBytes(tableName));
  }

  /**
   * Clear up an index's record in the table if its index data is missing.
   * <p>
   * This table is not needed to be disabled before calling this method, and after the return the
   * table will be enabled.
   * @param tableName
   * @throws IOException
   */
  public void clearupMissingIndexes(byte[] tableName) throws IOException {
    IndexTableDescriptor indexDesc = new IndexTableDescriptor(admin.getTableDescriptor(tableName));

    if (!indexDesc.hasIndex()) {
      return;
    }
    String errStr = "";
    try {
      admin.disableTable(tableName);
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (admin.tableExists(indexSpec.getIndexTableName())) {
          admin.disableTable(indexSpec.getIndexTableName());
        } else {
          errStr +=
              "[column:" + Bytes.toString(indexSpec.getIndexColumn()) + ",type:"
                  + indexSpec.getIndexType().toString() + "]";
          indexDesc.deleteIndex(indexSpec.getIndexColumn());
        }
      }
      admin.modifyTable(tableName, indexDesc.getTableDescriptor());

    } catch (IndexNotExistedException e) {
      e.printStackTrace();
    } catch (IOException e1) {
      if (!errStr.isEmpty()) {
        throw new IOException("Try to give up missing index " + errStr + " failed!", e1);
      }
    } finally {
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (admin.tableExists(indexSpec.getIndexTableName())) {
          admin.enableTable(indexSpec.getIndexTableName());
        }
      }
      admin.enableTable(tableName);
    }
  }

  /**
   * Get the KeyGen class for table's index key.
   * @param tableName
   * @return class
   * @throws IOException
   */
  public Class<? extends IndexKeyGenerator> getKeygenClass(byte[] tableName) throws IOException {
    HTableDescriptor desc = admin.getTableDescriptor(tableName);
    IndexTableDescriptor indexDesc = new IndexTableDescriptor(desc);
    return indexDesc.getKeygenClass();
  }

  /**
   * Set the KeyGen class for table's index key.
   * @param tableName
   * @param className
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public void setKeygenClass(byte[] tableName, String className) throws IOException,
      ClassNotFoundException {
    HTableDescriptor desc = admin.getTableDescriptor(tableName);
    IndexTableDescriptor indexDesc = new IndexTableDescriptor(desc);
    indexDesc.setKeygenClass(className);

    if (isTableEnabled(tableName)) {
      throw new IOException("Table " + Bytes.toString(tableName) + " is enabled! Disable it first!");
    }

    // modify base table
    admin.modifyTable(tableName, indexDesc.getTableDescriptor());
    // TODO
    // maybe need to enable and disable, check add indexes
  }

  /**
   * Set each column's data type of this table.
   * @param columnTypes
   * @throws IOException
   */
  public void setColumnInfoMap(byte[] tableName, Map<byte[], DataType> columnTypes)
      throws IOException {
    HTableDescriptor desc = admin.getTableDescriptor(tableName);

    if (isTableEnabled(tableName)) {
      throw new IOException("Table " + Bytes.toString(tableName) + " is enabled! Disable it first!");
    }

    StringBuilder sb = new StringBuilder();

    if (columnTypes != null && !columnTypes.isEmpty()) {
      int i = 0;
      for (Map.Entry<byte[], DataType> entry : columnTypes.entrySet()) {
        sb.append(Bytes.toString(entry.getKey()));
        sb.append(":");
        sb.append(entry.getValue().toString());
        if (i != columnTypes.size() - 1) {
          sb.append(",");
        }
        i++;
      }
    }

    desc.setValue("DATA_FORMAT", sb.toString());

    admin.modifyTable(tableName, desc);
    // TODO maybe need to enable and disable, check add indexes
  }

  /**
   * Get each column's data type of this table.
   * @return
   * @throws IOException
   */
  public Map<byte[], DataType> getColumnInfoMap(byte[] tableName) throws IOException {
    String tempInfo = admin.getTableDescriptor(tableName).getValue("DATA_FORMAT");

    Map<byte[], DataType> columnTypeMap = null;
    if (tempInfo != null) {
      columnTypeMap = new TreeMap<byte[], DataType>(Bytes.BYTES_COMPARATOR);

      String[] temp = tempInfo.split(",");
      for (int i = 0; i < temp.length; i++) {
        int loc = temp[i].lastIndexOf(':');
        if (loc != -1) {
          columnTypeMap.put(Bytes.toBytes(temp[i].substring(0, loc)),
            DataType.valueOf(temp[i].substring(loc + 1)));
        } else {
          LOG.warn("Failed to read column type!" + temp[i]);
        }
      }
    }

    return columnTypeMap;
  }

  /**
   * Add a column to an existing table. Asynchronous operation.
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final String tableName, HColumnDescriptor column) throws IOException {
    addColumn(Bytes.toBytes(tableName), column);
  }

  /**
   * Add a column to an existing table. Asynchronous operation.
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final byte[] tableName, HColumnDescriptor column) throws IOException {
    if (isTableEnabled(tableName)) {
      throw new IOException("Table " + Bytes.toString(tableName) + " is enabled! Disable it first!");
    }
    if (admin.getTableDescriptor(tableName).hasFamily(column.getName())) {
      return;
    }

    admin.addColumn(tableName, column);

    IndexTableDescriptor indexDesc = new IndexTableDescriptor(admin.getTableDescriptor(tableName));
    if (indexDesc.hasIndex()) {
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (indexSpec.getIndexType() == IndexType.CCINDEX) {
          admin.addColumn(indexSpec.getIndexTableName(), column);
        }
      }
    }
  }

  /**
   * Delete a column from a table. Asynchronous operation. If there is any index on this family,
   * then an exception will be thrown out.
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final String tableName, final String columnName) throws IOException {
    deleteColumn(Bytes.toBytes(tableName), Bytes.toBytes(columnName), false);
  }

  /**
   * Delete a column from a table. Asynchronous operation.
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @param force when there is any index on this family, if force is true, then delete index at the
   *          same time, otherwise an exception will be thrown out.
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final String tableName, final String columnName, final boolean force)
      throws IOException {
    deleteColumn(Bytes.toBytes(tableName), Bytes.toBytes(columnName), force);
  }

  /**
   * Delete a column from a table. Asynchronous operation. If there is any index on this family,
   * then an exception will be thrown out.
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final byte[] tableName, final byte[] columnName) throws IOException {
    deleteColumn(tableName, columnName, false);
  }

  /**
   * Delete a column from a table. Asynchronous operation.
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @param force when there is any index on this family, if force is true, then delete index at the
   *          same time, otherwise an exception will be thrown out.
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(final byte[] tableName, final byte[] columnName, final boolean force)
      throws IOException {
    if (isTableEnabled(tableName)) {
      throw new IOException("Table " + Bytes.toString(tableName) + " is enabled! Disable it first!");
    }
    if (!admin.getTableDescriptor(tableName).hasFamily(columnName)) {
      return;
    }

    IndexTableDescriptor indexDesc = new IndexTableDescriptor(admin.getTableDescriptor(tableName));
    if (!force && indexDesc.hasIndex()) {
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (Bytes.compareTo(indexSpec.getFamily(), columnName) == 0) {
          throw new IOException("Index [column:" + Bytes.toString(indexSpec.getIndexColumn())
              + ",type:" + indexSpec.getIndexType().toString()
              + "] is already existed on column family " + Bytes.toString(columnName)
              + ". Please delete index first!");
        }
      }
    }

    admin.deleteColumn(tableName, columnName);

    if (indexDesc.hasIndex()) {
      boolean modified = false;
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        if (Bytes.compareTo(indexSpec.getFamily(), columnName) == 0) {
          try {
            indexDesc.deleteIndex(indexSpec.getIndexColumn());
          } catch (IndexNotExistedException e) {
            e.printStackTrace();
          }

          if (admin.isTableEnabled(indexSpec.getIndexTableName())) {
            admin.disableTable(indexSpec.getIndexTableName());
          }
          admin.deleteTable(indexSpec.getIndexTableName());
          modified = true;
          continue;
        }

        if (indexSpec.getIndexType() == IndexType.CCINDEX) {
          admin.deleteColumn(indexSpec.getIndexTableName(), columnName);
        } else if (indexSpec.getIndexType() == IndexType.IMPSECONDARYINDEX) {
          if (indexSpec.getAdditionMap().containsKey(columnName)) {
            indexSpec.removeAdditionFamily(columnName);
            try {
              indexDesc.deleteIndex(indexSpec.getIndexColumn());
              indexDesc.addIndex(indexSpec);
            } catch (IndexNotExistedException e) {
              e.printStackTrace();
            } catch (IndexExistedException e) {
              e.printStackTrace();
            }
            modified = true;
            admin.deleteColumn(indexSpec.getIndexTableName(), columnName);
          }
        }
      }

      if (modified) {
        HTableDescriptor tdesc = indexDesc.getTableDescriptor();
        tdesc.removeFamily(columnName);
        admin.modifyTable(tableName, tdesc);
        admin.enableTable(tableName);
        admin.disableTable(tableName);
      }
    }
  }

  protected void setTest(boolean test) {
    this.isTest = test;
  }

  public void flushAll(byte[] tableName) throws IOException, InterruptedException {
    HTableDescriptor desc = admin.getTableDescriptor(tableName);
    if (isIndexTable(desc)) {
      throw new TableNotFoundException(Bytes.toString(tableName));
    }
    IndexTableDescriptor indexDesc = new IndexTableDescriptor(desc);

    if (indexDesc.hasIndex()) {
      for (IndexSpecification indexSpec : indexDesc.getIndexSpecifications()) {
        admin.flush(indexSpec.getIndexTableName());
      }
    }
    admin.flush(tableName);
  }
}