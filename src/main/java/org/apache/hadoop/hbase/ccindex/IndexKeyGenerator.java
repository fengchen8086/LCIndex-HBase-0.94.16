package org.apache.hadoop.hbase.ccindex;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

/**
 * Interface for creating index row key for a certain index row.
 * Each table may have a different strategy of generating index row key.
 * 
 * @author wanhao
 *
 */
public interface IndexKeyGenerator {
	/**
	 * Create a new row key for an index row.
	 * Used when put rows.
	 * @param indexSpec
	 * @param put
	 * @return index row key
	 */
	public byte[] createIndexRowKey(final IndexSpecification indexSpec, final Put put);
	
	/**
	 * Create a new row key for an index row.
	 * Used when delete whole row of index table.
	 * @param indexSpec
	 * @param result
	 * @return
	 */
	public byte[] createIndexRowKey(final IndexSpecification indexSpec, final Result result);
	
	/**
	 * Create a new row key for an index row.
	 * Used when delete part of a row of index table.
	 * @param indexSpec
	 * @param delete
	 * @return
	 */
	public byte[] createIndexRowKey(final IndexSpecification indexSpec, final Delete delete);
	
	/**
	 * Create a new row key for an index row.
	 * Used when you know the index column value clearly.
	 * @param rowKey
	 * @param value
	 * @return
	 */
	public byte[] createIndexRowKey(final byte[] rowKey, final byte[] value);
	
	/**
	 * Parse an index key to key of main data table and index column values.
	 * 
	 * @param indexKey
	 * @return byte[][]{rowKey, ColumnValue...}
	 */
	public byte[][] parseIndexRowKey(byte[] indexKey);
	
	public String toString();
}
