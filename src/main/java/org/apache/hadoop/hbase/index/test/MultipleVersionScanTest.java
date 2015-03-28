package org.apache.hadoop.hbase.index.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.IndexColumnDescriptor;
import org.apache.hadoop.hbase.index.client.IndexConstants;
import org.apache.hadoop.hbase.index.client.IndexDescriptor;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.index.client.RangeList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

public class MultipleVersionScanTest {
  static String tableName = "test-version";

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);

    HTable table = null;
    // if (admin.tableExists(tableName)) {
    // indexadmin.disableTable(tableName);
    // indexadmin.deleteTable(tableName);
    // }
    if (!admin.tableExists(tableName)) {
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);

      IndexDescriptor index1 = new IndexDescriptor(Bytes.toBytes("c1"), DataType.DOUBLE);

      IndexColumnDescriptor family = new IndexColumnDescriptor("f");
      family.addIndex(index1);

      tableDesc.addFamily(family);
      admin.createTable(tableDesc);

      table = new HTable(conf, tableName);
      Put put = new Put(Bytes.toBytes("001"));
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c1"), 10, Bytes.toBytes(120.0));
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c2"), 10, Bytes.toBytes("a"));
      table.put(put);

      put = new Put(Bytes.toBytes("001"));
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c1"), 20, Bytes.toBytes(90.0));
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c2"), 20, Bytes.toBytes("a"));
      table.put(put);

      put = new Put(Bytes.toBytes("002"));
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c1"), 30, Bytes.toBytes(150.0));
      put.add(Bytes.toBytes("f"), Bytes.toBytes("c2"), 30, Bytes.toBytes("a"));
      table.put(put);

    }

    admin.close();
    if (table == null) {
      table = new HTable(conf, tableName);
    }

    Scan scan = new Scan();
    RangeList list = new RangeList();
    list.addRange(new Range(Bytes.toBytes("f:c1"), Bytes.toBytes(100.0), CompareOp.GREATER, null,
        CompareOp.NO_OP));
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(Bytes.toBytes("f"), Bytes.toBytes("c1"), CompareOp.GREATER,
            Bytes.toBytes(100.0));
    filter.setLatestVersionOnly(false);

    scan.setFilter(filter);
//    scan.setAttribute(IndexConstants.SCAN_INDEX_RANGE, Writables.getBytes(list));
    scan.setMaxVersions();
    Result result = null;
    ResultScanner rs = table.getScanner(scan);

    while ((result = rs.next()) != null) {
      System.out.println(result);
    }
    rs.close();

    table.close();

  }
}
