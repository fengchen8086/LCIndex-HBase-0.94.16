package org.apache.hadoop.hbase.index.client;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Constants used in classed which are related to index.
 */
public final class IndexConstants {

  /**
   * regard base table as an index table on key(an index column name of base table)
   */
  public static final byte[] KEY = Bytes.toBytes("key");

  /**
   * minimum row key
   */
  public static final byte[] MIN_ROW_KEY = { (byte) 0x00 };

  public static final String REGION_INDEX_DIR_NAME = ".index";

  public static final String SCAN_WITH_INDEX="scan.with.index";
  
  public static final String MAX_SCAN_SCALE="max.scan.scale";
  
  public static final float DEFAULT_MAX_SCAN_SCALE= 0.5f;
  
  public static final String INDEXFILE_REPLICATION = "indexfile.replication";
  
  public static final int DEFAULT_INDEXFILE_REPLICATION = 3;
  
  public static final String HREGION_INDEX_COMPACT_CACHE_MAXSIZE = "hregion.index.compact.cache.maxsize";
  
  public static final long DEFAULT_HREGION_INDEX_COMPACT_CACHE_MAXSIZE = 67108864L;
  
  public static final String INDEX_COMPACTION_LOCAL_DIR="index.compaction.local.dir";
}
