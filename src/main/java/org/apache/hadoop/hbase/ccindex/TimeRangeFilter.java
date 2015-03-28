//package org.apache.hadoop.hbase.filter;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.util.List;
//import java.util.TreeSet;
//
//import org.apache.hadoop.hbase.KeyValue;
//
///**
// * Filter that returns only cells whose timestamp (version) is
// * in the specified range of timestamps (versions).
// * <p>
// * Note: Use of this filter overrides any time range/time stamp
// * options specified using {@link org.apache.hadoop.hbase.client.Get#setTimeRange(long, long)},
// * {@link org.apache.hadoop.hbase.client.Scan#setTimeRange(long, long)}, {@link org.apache.hadoop.hbase.client.Get#setTimeStamp(long)},
// * or {@link org.apache.hadoop.hbase.client.Scan#setTimeStamp(long)}.
// * @author Liu Jia
// */
//public class TimeRangeFilter extends FilterBase {
//
//
//  // Used during scans to hint the scan to stop early
//  // once the timestamps fall below the minTimeStamp.
//  long minTimeStamp = Long.MIN_VALUE;
//  long maxTimeStamp = Long.MAX_VALUE;
//
//  /**
//   * Used during deserialization. Do not use otherwise.
//   */
//  public TimeRangeFilter() {
//    super();
//  }
//
//  /**
//   * Constructor for filter that retains only those
//   * cells whose timestamp (version) is in the specified
//   * list of timestamps.
//   *
//   * @param timestamps
//   */
//  public TimeRangeFilter(long mixTimeStamp,long maxTimeStamp) {
//    if(mixTimeStamp!=-1)
//    {
//      this.minTimeStamp=minTimeStamp;
//    }
//    if(maxTimeStamp!=-1)
//    {
//      this.maxTimeStamp=maxTimeStamp;
//    }
//  }
//
//
//  /**
//   * Gets the minimum timestamp requested by filter.
//   * @return  minimum timestamp requested by filter.
//   */
//  public long getMin() {
//    return minTimeStamp;
//  }
//  
//  public long getMax()
//  {
//    return maxTimeStamp;
//  }
//
//  @Override
//  public ReturnCode filterKeyValue(KeyValue v) {
//    if (v.getTimestamp()<this.maxTimeStamp&&v.getTimestamp()>this.minTimeStamp) {
//      return ReturnCode.INCLUDE;
//    } else if (v.getTimestamp() < minTimeStamp) {
//      // The remaining versions of this column are guaranteed
//      // to be lesser than all of the other values.
//      return ReturnCode.NEXT_COL;
//    }
//    return ReturnCode.SKIP;
//  }
//
//  @Override
//  public void readFields(DataInput in) throws IOException {
//    this.maxTimeStamp=in.readLong();
//    this.minTimeStamp=in.readLong();
//  }
//
//  @Override
//  public void write(DataOutput out) throws IOException {
//    out.writeLong(this.maxTimeStamp);
//    out.writeLong(this.minTimeStamp);
//  }
//}

/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ccindex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This filter is used to filter cells based on value. It takes a
 * {@link CompareFilter.CompareOp} operator (equal, greater, not equal, etc),
 * and either a byte [] value or a WritableByteArrayComparable.
 * <p>
 * If we have a byte [] value then we just do a lexicographic compare. For
 * example, if passed value is 'b' and cell has 'a' and the compare operator is
 * LESS, then we will filter out this cell (return true). If this is not
 * sufficient (eg you want to deserialize a long and then compare it to a fixed
 * long value), then you can pass in your own comparator instead.
 * <p>
 * You must also specify a family and qualifier. Only the value of this column
 * will be tested. When using this filter on a {@link Scan} with specified
 * inputs, the column to be tested should also be added as input (otherwise the
 * filter will regard the column as missing).
 * <p>
 * To prevent the entire row from being emitted if the column is not found on a
 * row, use {@link #setFilterIfMissing}. Otherwise, if the column is found, the
 * entire row will be emitted only if the value passes. If the value fails, the
 * row will be filtered out.
 * <p>
 * In order to test values of previous versions (timestamps), set
 * {@link #setLatestVersionOnly} to false. The default is true, meaning that
 * only the latest version's value is tested and all previous versions are
 * ignored.
 * <p>
 * To filter based on the value of all scanned columns, use {@link ValueFilter}.
 */
public class TimeRangeFilter extends FilterBase {
  static final Log LOG = LogFactory.getLog(SingleColumnValueFilter.class);

  protected byte[] columnFamily;
  protected byte[] columnQualifier;
  private long startTs = Long.MIN_VALUE;
  private long endTs = Long.MAX_VALUE;
  private boolean foundColumn = false;
  private boolean filterIfMissing = false;
  private boolean latestVersionOnly = true;

  /**
   * Writable constructor, do not use.
   */
  public TimeRangeFilter() {
  }

  /**
   * 
   * @param family 
   * @param qualifier
   * @param startTs
   * @param endTs
   */
  public TimeRangeFilter(final byte[] family, final byte[] qualifier,
      final long startTs, final long endTs) {
    this.columnFamily = family;
    this.columnQualifier = qualifier;
    if (startTs != -1)
      this.startTs = startTs;
    if (endTs != -1)
      this.endTs = endTs;
  }

  /**
   * @return the family
   */
  public byte[] getFamily() {
    return columnFamily;
  }

  /**
   * @return the qualifier
   */
  public byte[] getQualifier() {
    return columnQualifier;
  }

  public ReturnCode filterKeyValue(KeyValue keyValue) {
    if(keyValue.matchingColumn(this.columnFamily, this.columnQualifier))
    {
      
      if(keyValue.getTimestamp()<this.startTs)
      {
        return ReturnCode.NEXT_COL;
      }
      else if(keyValue.getTimestamp()>=this.startTs&&keyValue.getTimestamp()<=this.endTs)
      {
    	  	this.foundColumn=true;
        return ReturnCode.INCLUDE;
      }
      else
      {
        return ReturnCode.SKIP;
      }
    }else
    {
    	return ReturnCode.INCLUDE;
    }
//    if (this.timestamps.contains(v.getTimestamp())) {
//      return ReturnCode.INCLUDE;
//    } else if (v.getTimestamp() < minTimeStamp) {
//      // The remaining versions of this column are guaranteed
//      // to be lesser than all of the other values.
//      return ReturnCode.NEXT_COL;
//    }
//    return ReturnCode.SKIP;
//    // System.out.println("REMOVE KEY=" + keyValue.toString() + ", value=" +
//    // Bytes.toString(keyValue.getValue()));
//    if (this.matchedColumn) {
//      // We already found and matched the single column, all keys now pass
//      return ReturnCode.INCLUDE;
//    } else if (this.latestVersionOnly && this.foundColumn) {
//      // We found but did not match the single column, skip to next row
//      return ReturnCode.NEXT_ROW;
//    }
//    if (!keyValue.matchingColumn(this.columnFamily, this.columnQualifier)) {
//      return ReturnCode.INCLUDE;
//    }
//    foundColumn = true;
//    if (keyValue.getTimestamp()>=this.startTs&&keyValue.getTimestamp()<=this.endTs) {
//      this.matchedColumn = true;
//      return ReturnCode.INCLUDE;
//    }
//    else
//    {
//      if(this.latestVersionOnly)
//      {
//        return ReturnCode.NEXT_ROW;
//      }
//      else
//      {
//        return ReturnCode.SKIP;
//      }
//    }
  }

  public boolean filterRow() {
    // If column was found, return false if it was matched, true if it was not
    // If column not found, return true if we filter if missing, false if not
    return this.foundColumn ? false : this.filterIfMissing;
  }

  public void reset() {
    foundColumn = false;
  }

  /**
   * Get whether entire row should be filtered if column is not found.
   * 
   * @return true if row should be skipped if column not found, false if row
   *         should be let through anyways
   */
  public boolean getFilterIfMissing() {
    return filterIfMissing;
  }

  /**
   * Set whether entire row should be filtered if column is not found.
   * <p>
   * If true, the entire row will be skipped if the column is not found.
   * <p>
   * If false, the row will pass if the column is not found. This is default.
   * 
   * @param filterIfMissing
   *          flag
   */
  public void setFilterIfMissing(boolean filterIfMissing) {
    this.filterIfMissing = filterIfMissing;
  }

  /**
   * Get whether only the latest version of the column value should be compared.
   * If true, the row will be returned if only the latest version of the column
   * value matches. If false, the row will be returned if any version of the
   * column value matches. The default is true.
   * 
   * @return return value
   */
  public boolean getLatestVersionOnly() {
    return latestVersionOnly;
  }

  /**
   * Set whether only the latest version of the column value should be compared.
   * If true, the row will be returned if only the latest version of the column
   * value matches. If false, the row will be returned if any version of the
   * column value matches. The default is true.
   * 
   * @param latestVersionOnly
   *          flag
   */
  public void setLatestVersionOnly(boolean latestVersionOnly) {
    this.latestVersionOnly = latestVersionOnly;
  }

  public void readFields(final DataInput in) throws IOException {
    this.columnFamily = Bytes.readByteArray(in);
    if (this.columnFamily.length == 0) {
      this.columnFamily = null;
    }
    this.columnQualifier = Bytes.readByteArray(in);
    if (this.columnQualifier.length == 0) {
      this.columnQualifier = null;
    }
    this.startTs=in.readLong();
    this.endTs=in.readLong();

    this.foundColumn = in.readBoolean();
    this.filterIfMissing = in.readBoolean();
    this.latestVersionOnly = in.readBoolean();
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.columnFamily);
    Bytes.writeByteArray(out, this.columnQualifier);
    out.writeLong(this.startTs);
    out.writeLong(this.endTs);
    out.writeBoolean(foundColumn);

    out.writeBoolean(filterIfMissing);
    out.writeBoolean(latestVersionOnly);
  }
}
