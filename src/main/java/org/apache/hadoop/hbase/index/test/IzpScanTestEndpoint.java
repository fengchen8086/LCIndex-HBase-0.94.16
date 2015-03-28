/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.index.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;

public class IzpScanTestEndpoint extends BaseEndpointCoprocessor implements IzpScanTestProtocol {
  private static final Log LOG = LogFactory.getLog(IzpScanTestEndpoint.class);

  @Override
  public Integer scan(Scan scan, String startTime, String stopTime, boolean printResult) {
    HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    byte[] startkey = region.getStartKey();
    int count = 0;

    if (startkey != null && startkey.length != 0) {
      scan.setStartRow(Bytes.add(startkey, Bytes.toBytes(startTime)));
      scan.setStopRow(Bytes.add(startkey, Bytes.toBytes(stopTime)));

      List<KeyValue> values = new ArrayList<KeyValue>();
      try {
        RegionScanner rs = region.getScanner(scan);
        while (true) {
          boolean moreRows = rs.nextRaw(values, SchemaMetrics.METRIC_NEXTSIZE);
          if (!values.isEmpty()) {
            count++;
            if (printResult) {
              println(new Result(values));
            }
          }
          if (!moreRows) {
            break;
          }
          values.clear();
        }
        rs.close();

      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    LOG.debug("region:" + Bytes.toString(startkey) + ", results:" + count);

    return count;
  }

  @Override
  public Result[] scan(Scan scan, String startTime, String stopTime) {
    HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    byte[] startkey = region.getStartKey();
    int count = 0;
    List<Result> results = new ArrayList<Result>();

    if (startkey != null && startkey.length != 0) {
      scan.setStartRow(Bytes.add(startkey, Bytes.toBytes(startTime)));
      scan.setStopRow(Bytes.add(startkey, Bytes.toBytes(stopTime)));

      List<KeyValue> values = new ArrayList<KeyValue>();
      try {
        RegionScanner rs = region.getScanner(scan);
        while (true) {
          boolean moreRows = rs.nextRaw(values, SchemaMetrics.METRIC_NEXTSIZE);
          if (!values.isEmpty()) {
            count++;
            results.add(new Result(values));
          }
          if (!moreRows) {
            break;
          }
          values.clear();
        }
        rs.close();

      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    LOG.debug("region:" + Bytes.toString(startkey) + ", results:" + count);

    return results.toArray(new Result[results.size()]);
  }

  static void println(Result result) {
    StringBuilder sb = new StringBuilder();
    // f FLOW_TYPE String
    // h HOST String
    // a AID String
    // y AP_TYPE String
    // r REGION_ID String
    // g AD_GROUP_ID String
    // p AD_PLAN_ID String
    // o OWNER_ID String
    // s SIZE Int
    // w SHOW Int
    // c CLICK Int
    // p PUSH Int
    // i IP Int
    // u UV Int
    // c COST Double
    sb.append("row=" + Bytes.toString(result.getRow()));

    List<KeyValue> kv = null;

    String string_column[] = { "f", "h", "a", "y", "r", "g", "p", "o" };
    String int_column1[] = { "s", "w", "c" };
    String int_column2[] = { "p", "i", "u" };
    for (String column : string_column) {
      kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes(column));
      if (kv.size() != 0) {
        sb.append(", " + Bytes.toString(kv.get(0).getFamily()) + ":" + column + "="
            + Bytes.toString(kv.get(0).getValue()));
      }
    }

    for (String column : int_column1) {
      kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes(column));
      if (kv.size() != 0) {
        sb.append(", " + Bytes.toString(kv.get(0).getFamily()) + ":" + column + "="
            + Bytes.toInt(kv.get(0).getValue()));
      }
    }

    for (String column : int_column2) {
      kv = result.getColumn(Bytes.toBytes("q"), Bytes.toBytes(column));
      if (kv.size() != 0) {
        sb.append(", " + Bytes.toString(kv.get(0).getFamily()) + ":" + column + "="
            + Bytes.toInt(kv.get(0).getValue()));
      }
    }

    kv = result.getColumn(Bytes.toBytes("q"), Bytes.toBytes("c"));
    if (kv.size() != 0) {
      sb.append(", " + Bytes.toString(kv.get(0).getFamily()) + ":" + "c" + "="
          + Bytes.toDouble(kv.get(0).getValue()));
    }

    LOG.debug(sb.toString());
  }
}