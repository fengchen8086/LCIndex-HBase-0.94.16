package tpch.put;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TPCHRecord {

  private final String DELIMITER = "|";
  private String orderKey;
  private long custKey;
  private String status;
  private double totalPrice;
  private long date;
  private String priority;
  private String clerk;
  private int shipPriority;
  private String comment;

  private final DateFormat from = new SimpleDateFormat("yyyy-MM-dd");
  private final DateFormat to = new SimpleDateFormat("yyyyMMdd");

  public TPCHRecord(String str) throws ParseException {
    int currentPos = 0;
    String temp;
    int index = 0;
    while (true) {
      index = str.indexOf(DELIMITER);
      if (index < 0) break;
      temp = str.substring(0, index);
      str = str.substring(index + 1);
      switch (currentPos) {
      case 0:
        orderKey = temp;
        break;
      case 1:
        custKey = Long.valueOf(temp);
        break;
      case 2:
        status = temp;
        break;
      case 3:
        totalPrice = Double.valueOf(temp);
        break;
      case 4:
        // date
        String s = to.format(from.parse(temp));
        date = Integer.valueOf(s);
        break;
      case 5:
        priority = temp;
        break;
      case 6:
        clerk = temp;
        break;
      case 7:
        shipPriority = Integer.valueOf(temp);
        break;
      case 8:
        comment = temp;
        break;
      default:
      }
      ++currentPos;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(orderKey).append(DELIMITER);
    sb.append(custKey).append(DELIMITER);
    sb.append(status).append(DELIMITER);
    sb.append(totalPrice).append(DELIMITER);
    sb.append(date).append(DELIMITER);
    sb.append(priority).append(DELIMITER);
    sb.append(clerk).append(DELIMITER);
    sb.append(shipPriority).append(DELIMITER);
    sb.append(comment).append(DELIMITER);
    return sb.toString();
  }

  public Put getPut(String familyName) {
    Put put = new Put(Bytes.toBytes(orderKey));
    put.add(Bytes.toBytes(familyName), Bytes.toBytes("ck"), Bytes.toBytes(custKey));
    put.add(Bytes.toBytes(familyName), Bytes.toBytes("st"), Bytes.toBytes(status));
    put.add(Bytes.toBytes(familyName), Bytes.toBytes("t"), Bytes.toBytes(totalPrice));
    put.add(Bytes.toBytes(familyName), Bytes.toBytes("d"), Bytes.toBytes(date));
    put.add(Bytes.toBytes(familyName), Bytes.toBytes("p"), Bytes.toBytes(priority));
    put.add(Bytes.toBytes(familyName), Bytes.toBytes("cl"), Bytes.toBytes(clerk));
    put.add(Bytes.toBytes(familyName), Bytes.toBytes("sh"), Bytes.toBytes(shipPriority));
    put.add(Bytes.toBytes(familyName), Bytes.toBytes("cm"), Bytes.toBytes(comment));
    return put;
  }

  public double getTotalPrice() {
    return totalPrice;
  }

  public long getDate() {
    return date;
  }

  public String getPriority() {
    return priority;
  }

  public String getOrderKey() {
    return orderKey;
  }
}
