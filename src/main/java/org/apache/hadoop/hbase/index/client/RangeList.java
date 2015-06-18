package org.apache.hadoop.hbase.index.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * Holds an array of index range.
 */
public class RangeList implements Writable {

  private ArrayList<Range> ranges = new ArrayList<Range>();

  public RangeList() {
  }

  public RangeList(List<Range> list) {
    this.ranges.addAll(list);
  }

  public void addRange(Range r) {
    this.ranges.add(r);
  }

  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    ranges = new ArrayList<Range>(size);
    for (int i = 0; i < size; i++) {
      Range r = new Range();
      r.readFields(in);
      ranges.add(r);
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(ranges.size());
    for (Range r : ranges) {
      r.write(out);
    }
  }

  public ArrayList<Range> getRangeList() {
    return ranges;
  }

  public void removeRange(Range r) {
    if (r == null) return;
    if (!ranges.contains(r)) {
      System.out.println("winter in rangelist not contain such range: " + r);
      return;
    }
    ranges.remove(r);
    
  }
}
