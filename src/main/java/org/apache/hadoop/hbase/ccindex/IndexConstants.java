package org.apache.hadoop.hbase.ccindex;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Constants used in classed which are related to index.
 * 
 * @author wanhao
 *
 */
public final class IndexConstants {
	/**
	 * refers to a value which describes indexes and is stored in main data table description
	 */
	public static final byte[] INDEX_KEY = Bytes.toBytes("INDEX_KEY");
	
	/**
	 * refers to the key generator class name
	 */
	public static final byte[] KEYGEN = Bytes.toBytes("KEYGEN");
	
	/**
	 * refers to a value which is the flag of main data table, stored in main data table description
	 */
	public static final byte[] BASE_KEY = Bytes.toBytes("BASE_KEY");

	/**
	 * index type:ccindex, secondary index and improved secondary index
	 */
	public static final byte[] INDEX_TYPE=Bytes.toBytes("INDEX_TYPE");
	
	/**
	 * regard base table as an index table on key(an index column name of base table)
	 */
	public static final byte[] KEY = Bytes.toBytes("key");
	
	/**
	 * zero series used to construct the last part of a index key 
	 */
	public static final String LASTPART_ZERO = "00000000";
	
	/**
	 * string length of last part of a index key, 
	 * the last part is a combination of several '0' with the length of the indexed column value 
	 */
	public static final int LASTPART_LENGTH = LASTPART_ZERO.length();

	/**
	 * minimum row key
	 */
	public static final byte[] MIN_ROW_KEY={(byte) 0x00};
	
	/**
	 * maximum row key
	 */
	public static final byte[] MAX_ROW_KEY={(byte) 0xff};
	
	/**
	 * minimum family name used in SecIndex and ImpSecIndex
	 */
	public static final byte[] MIN_FAMILY_NAME=new byte[]{48};
}
