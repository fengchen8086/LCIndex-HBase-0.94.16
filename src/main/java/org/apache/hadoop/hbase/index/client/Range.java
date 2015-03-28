package org.apache.hadoop.hbase.index.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Structure to store one certain column's query range.
 */
public class Range implements Writable {
  private byte[] column = null;
  private byte[] startValue = null;
  private byte[] stopValue = null;
  private CompareOp startType;
  private CompareOp stopType;
  private DataType dataType = DataType.STRING;

  public Range() {

  }

  /**
   * Construct a Range whose start value and end value are null.
   * @param column
   */
  public Range(byte[] column) {
    this(column, null, CompareOp.NO_OP, null, CompareOp.NO_OP);
  }

  /**
   * Construct a Range with given parameters.
   * @param column
   * @param startValue
   * @param startType - can only be EQUAL, GREATER ,GREATER_OR_EQUAL or NO_OP
   * @param endValue
   * @param endType - can only be LESS , LESS_OR_EQUAL or NO_OP
   */
  public Range(byte[] column, byte[] startValue, CompareOp startType, byte[] endValue,
      CompareOp endType) {
    setColumn(column);
    this.startValue = startValue;
    setStartType(startType);
    this.stopValue = endValue;
    setStopType(endType);
  }

  public Range(byte[] column, DataType dataType, byte[] startValue, CompareOp startType,
      byte[] endValue, CompareOp endType) {
    setColumn(column);
    this.dataType = dataType;
    this.startValue = startValue;
    setStartType(startType);
    this.stopValue = endValue;
    setStopType(endType);
  }

  public DataType getDataType() {
    return dataType;
  }

  public CompareOp getStartType() {
    return this.startType;
  }

  /**
   * Set start type for start value.
   * @param startType - can only be EQUAL, GREATER, GREATER_OR_EQUAL or NO_OP
   */
  public void setStartType(CompareOp startType) {
    if (startType == CompareOp.LESS || startType == CompareOp.LESS_OR_EQUAL
        || startType == CompareOp.NOT_EQUAL) {
      throw new IllegalArgumentException("Illegal start type: " + startType.toString());
    }
    this.startType = startType;
  }

  public CompareOp getStopType() {
    return this.stopType;
  }

  /**
   * Set end type for end value.
   * @param stopType - can only be LESS , LESS_OR_EQUAL or NO_OP
   */
  public void setStopType(CompareOp stopType) {
    if (stopType == CompareOp.EQUAL || stopType == CompareOp.GREATER
        || stopType == CompareOp.GREATER_OR_EQUAL || stopType == CompareOp.NOT_EQUAL) {
      throw new IllegalArgumentException("Illegal end type: " + stopType.toString());
    }

    this.stopType = stopType;
  }

  public byte[] getColumn() {
    return column;
  }

  private byte[] family = null;
  private byte[] qualifier = null;

  public byte[] getFamily() {
    return family == null ? (family = KeyValue.parseColumn(column)[0]) : family;
  }

  public byte[] getQualifier() {
    return qualifier == null ? (qualifier = KeyValue.parseColumn(column)[1]) : qualifier;
  }

  /**
   * Set column name for the Range.
   * @param column
   */
  public void setColumn(byte[] column) {
    if (column == null || column.length == 0) {
      throw new IllegalArgumentException("Column name is empty!");
    }
    this.column = column;
  }

  public byte[] getStartValue() {
    return startValue;
  }

  /**
   * Set start value for the Range.
   * @param startValue
   */
  public void setStartValue(byte[] startValue) {
    this.startValue = startValue;
  }

  public byte[] getStopValue() {
    return stopValue;
  }

  /**
   * Set end value for the Range.
   * @param stopValue
   */
  public void setStopValue(byte[] stopValue) {
    this.stopValue = stopValue;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();

    if (startValue != null && startValue.length != 0 && startType != null) {
      sb.append(Bytes.toString(column));
      switch (startType) {
      case EQUAL:
        sb.append("=");
        break;
      case GREATER_OR_EQUAL:
        sb.append(">=");
        break;
      case GREATER:
        sb.append(">");
        break;
      case NOT_EQUAL:
        sb.append("!=");
        break;
      default:
        break;
      }
      sb.append(Bytes.toStringBinary(startValue));
    }

    if (startValue != null && startValue.length != 0 && startType != null && stopValue != null
        && stopValue.length != 0 && stopType != null) {
      sb.append(" AND ");
    }

    if (stopValue != null && stopValue.length != 0 && stopType != null) {
      sb.append(Bytes.toString(column));

      switch (stopType) {
      case LESS_OR_EQUAL:
        sb.append("<=");
        break;
      case LESS:
        sb.append("<");
        break;
      default:
        break;
      }
      sb.append(Bytes.toStringBinary(stopValue));
    }
    return sb.toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    column = Bytes.readByteArray(in);
    dataType = WritableUtils.readEnum(in, DataType.class);
    if (in.readBoolean()) {
      startType = WritableUtils.readEnum(in, CompareOp.class);
      startValue = Bytes.readByteArray(in);
    } else {
      startType = CompareOp.NO_OP;
      startValue = null;
    }
    if (in.readBoolean()) {
      stopType = WritableUtils.readEnum(in, CompareOp.class);
      stopValue = Bytes.readByteArray(in);
    } else {
      stopType = CompareOp.NO_OP;
      stopValue = null;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, column);
    WritableUtils.writeEnum(out, dataType);
    if (startValue != null) {
      out.writeBoolean(true);
      WritableUtils.writeEnum(out, startType);
      Bytes.writeByteArray(out, startValue);
    } else {
      out.writeBoolean(false);
    }

    if (stopValue != null) {
      out.writeBoolean(true);
      WritableUtils.writeEnum(out, stopType);
      Bytes.writeByteArray(out, stopValue);
    } else {
      out.writeBoolean(false);
    }

  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Range)) {
      return false;
    }
    Range r = (Range) obj;
    if (Bytes.compareTo(column, r.getColumn()) != 0) {
      return false;
    }

    if (startValue == null) {
      if (startValue != r.getStartValue()) {
        return false;
      }
    } else {
      if (r.getStartValue() == null || Bytes.compareTo(startValue, r.getStartValue()) != 0
          || startType != r.getStartType()) {
        return false;
      }
    }

    if (stopValue == null) {
      if (stopValue != r.getStopValue()) {
        return false;
      }
    } else {
      if (r.getStopValue() == null || Bytes.compareTo(stopValue, r.getStopValue()) != 0
          || stopType != r.getStopType()) {
        return false;
      }
    }

    return true;
  }

  public static Range[] fromFilter(SingleColumnValueFilter filter) {
    if (!(filter.getComparator() instanceof BinaryComparator)) {
      return new Range[0];
    }

    byte[] column = KeyValue.makeColumn(filter.getFamily(), filter.getQualifier());
    CompareOp compareOp = filter.getOperator();
    byte[] value = filter.getComparator().getValue();

    if (compareOp == CompareOp.NOT_EQUAL) {
      return new Range[] { new Range(column, null, CompareOp.NO_OP, value, CompareOp.LESS),
          new Range(column, value, CompareOp.GREATER, null, CompareOp.NO_OP) };
    } else {
      switch (compareOp) {
      case EQUAL:
      case GREATER_OR_EQUAL:
      case GREATER:
        return new Range[] { new Range(column, value, compareOp, null, CompareOp.NO_OP) };
      case LESS:
      case LESS_OR_EQUAL:
        return new Range[] { new Range(column, null, CompareOp.NO_OP, value, compareOp) };
      default:
        return new Range[0];
      }
    }
  }
}
