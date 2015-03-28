package org.apache.hadoop.hbase.ccindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Simple index chooser that decide which index table to scan.
 * @author wanhao
 */
public class SimpleIndexChooser extends IndexChooser {
  public SimpleIndexChooser(final IndexTable indexTable) throws IOException {
    super(indexTable);
  }

  public int whichToScan(Range[] ranges, byte[][] resultColumns) throws IOException {
    if (ranges == null || ranges.length == 0) {
      throw new IllegalArgumentException("Range array is null or empty!");
    } else {
      ArrayList<Integer> indexList = new ArrayList<Integer>();
      for (int i = 0; i < ranges.length; i++) {
        // if an index range has timestamp, ignore it
        if (this.indexRegionMaps.containsKey(ranges[i].getColumn()) && ranges[i].getEndTs() == -1
            && ranges[i].getStartTs() == -1) {
          indexList.add(new Integer(i));
        }
      }
      // Contains no index column, use default key column to scan the whole main data table
      if (indexList.size() == 0) {
        return -1;
      } else {
        float[] rangeScore = new float[indexList.size()];

        for (int i = 0; i < indexList.size(); i++) {
          rangeScore[i] = this.getRangeDistance(ranges[indexList.get(i).intValue()]);
          LOG.debug(Bytes.toString(ranges[indexList.get(i).intValue()].getColumn())
              + " gets socre:" + rangeScore[i]);
          if (!containAllResultColumns(ranges[indexList.get(i).intValue()].getColumn(),
            resultColumns)) {
            rangeScore[i] *= speedTimes;
          }
        }
        int min = this.getMin(rangeScore);

        // if minimum index get a score more than 1, then scan the whole main data table instead
        if (rangeScore[min] <= 1) {
          return indexList.get(min).intValue();
        } else {
          return -1;
        }
      }
    }
  }

  /**
   * Get position of the minimum value from the float array.
   * @param value
   * @return
   */
  private int getMin(final float[] value) {
    int minFlag = 0;
    float min = Float.MAX_VALUE;

    for (int i = 0; i < value.length; i++) {
      if (min >= value[i]) {
        min = value[i];
        minFlag = i;
      }
    }
    return minFlag;
  }

  /**
   * Get the distance between a range's start value and its end value, represented by the percentage
   * of HRegions which a range spans in all the HRegions of the index table.
   * @param range
   * @return
   * @throws IOException-if index table has no region information
   */
  public float getRangeDistance(Range range) throws IOException {
    if (range.getStartType() == CompareOp.EQUAL) {
      return -1.0f;
    }

    byte[] startKey = range.getStartValue();
    byte[] endKey = range.getEndValue();

    List<HRegionInfo> infos = indexRegionMaps.get(range.getColumn());
    if (infos == null || infos.size() == 0) {
      throw new IOException("Index table of " + Bytes.toString(range.getColumn())
          + " has no any HRegionInfo!");
    }

    int startRegion = 0;
    int endRegion = infos.size() - 1;

    for (int m = 0; m < infos.size(); m++) {
      HRegionInfo temp = infos.get(m);
      // TODO region start end key may be null
      if (startKey != null && startKey.length != 0) {
        if (temp.containsRow(startKey)) {
          startRegion = m;
        }
      }
      if (endKey != null && endKey.length != 0) {
        if (temp.containsRow(endKey)) {
          endRegion = m;
        }
      }
    }

    // regions between start region and end region (inclusive)
    int dis = (endRegion - startRegion - 1);

    // region percent between startKey and the region's endKey in the whole region
    float startdis = 1.0f;
    if (startKey != null) {
      startdis =
          1 - getByteArrayDistance(infos.get(startRegion).getStartKey(), infos.get(startRegion)
              .getEndKey(), startKey);
    }

    // region percent between endKey and the region's startKey in the whole region
    float enddis = 1.0f;
    if (endKey != null) {
      enddis =
          getByteArrayDistance(infos.get(endRegion).getStartKey(),
            infos.get(endRegion).getEndKey(), endKey);
    }

    return (dis + startdis + enddis) / infos.size();
  }

  /**
   * Get the distance between start and value, represented by the percentage of distance between
   * start and value in distance between start and end. It is calculated by comparing byte by byte.
   * @param start
   * @param end
   * @param value
   * @throws IllegalArgumentException-if value is not between start and end
   * @return location percentage
   */
  public float getByteArrayDistance(byte[] start, byte[] end, byte[] value) {
    if (value == null) {
      throw new IllegalArgumentException("Value is null!");
    }
    int maxLen = Integer.MIN_VALUE;
    if (start != null) {
      maxLen = start.length > maxLen ? start.length : maxLen;
    }
    if (end != null) {
      maxLen = end.length > maxLen ? end.length : maxLen;
    }
    maxLen = value.length > maxLen ? value.length : maxLen;

    if (start == null || start.length == 0) {
      start = new byte[maxLen];
      for (int i = 0; i < maxLen; i++) {
        start[i] = (byte) 0x00;
      }
    }
    if (end == null || end.length == 0) {
      end = new byte[maxLen];
      for (int i = 0; i < maxLen; i++) {
        end[i] = (byte) 0xff;
      }
    }

    int startInt = 0;
    int endInt = 0;
    int valueInt = 0;
    int difcount = 0;

    for (int i = 0; i < maxLen; i++) {
      if (difcount >= 3) {
        break;
      }
      int starttmp = 0x00;
      int endtmp = 0x00;
      int valuetmp = 0x00;

      if (start.length > i) {
        starttmp = (start[i] & 0xff);
      }

      if (end.length > i) {
        endtmp = (end[i] & 0xff);
      }

      if (value.length > i) {
        valuetmp = (value[i] & 0xff);
      }
      startInt = startInt * 256 + starttmp;
      endInt = endInt * 256 + endtmp;
      valueInt = valueInt * 256 + valuetmp;

      if ((valueInt >= startInt) && (valueInt <= endInt)) {
        if (startInt != valueInt || endInt != valueInt) {
          difcount++;
        }
      } else {
        throw new IllegalArgumentException("value is not between start and end!");
      }
    }

    return 1.0f * (valueInt - startInt) / (endInt - startInt);
  }

}