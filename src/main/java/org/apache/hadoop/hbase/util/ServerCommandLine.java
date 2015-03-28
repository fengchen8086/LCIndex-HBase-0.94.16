/**
 * Copyright 2010 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF)
 * under one or more contributor license agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership. The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ManagementFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Base class for command lines that start up various HBase daemons.
 */
public abstract class ServerCommandLine extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(ServerCommandLine.class);

  /**
   * Implementing subclasses should return a usage string to print out.
   */
  protected abstract String getUsage();

  /**
   * Print usage information for this command line.
   * @param message if not null, print this message before the usage info.
   */
  protected void usage(String message) {
    if (message != null) {
      System.err.println(message);
      System.err.println("");
    }

    System.err.println(getUsage());
  }

  /**
   * Log information about the currently running JVM.
   */
  public static void logJVMInfo() {
    // Print out vm stats before starting up.
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    if (runtime != null) {
      LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" + runtime.getVmVendor()
          + ", vmVersion=" + runtime.getVmVersion());
      LOG.info("vmInputArguments=" + runtime.getInputArguments());
    }
  }

  /**
   * Parse and run the given command line. This may exit the JVM if a nonzero exit code is returned
   * from <code>run()</code>.
   */
  public void doMain(String args[]) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String fileName = "/home/winter/softwares/hbase-0.94.16/conf/winter-assign";
    File file = new File(fileName);
    if (file.exists()) {
      System.out.println("winter load newly defined values from file: " + fileName);
      parseMannuallyAssignedFile(conf, fileName);
    }

    int ret = ToolRunner.run(conf, this, args);
    if (ret != 0) {
      System.exit(ret);
    }
  }

  public static void parseMannuallyAssignedFile(Configuration conf, String file) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    String parts[] = null;
    while ((line = br.readLine()) != null) {
      if (line.startsWith("#")) {
        System.out.println("skip line: " + line);
        continue;
      }
      parts = line.split("\t");
      if (parts.length != 2) {
        System.out.println("line error: " + line);
        continue;
      }
      System.out.println("setting line: " + line);
      conf.set(parts[0], parts[1]);
    }
    br.close();
  }
}
