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

package org.apache.hadoop.hbase.index.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;

/**
 * KeyValueScanner adaptor over the Reader. It also provides hooks into bloom filter things.
 */
public class StoreFileIndexScanner implements IndexKeyValueScanner {
  static final Log LOG = LogFactory.getLog(Store.class);

  // the reader it comes from:
  private final IndexReader reader;
  private final IndexFileScanner ifs;
  private IndexKeyValue cur = null;

  /**
   * Implements a {@link IndexKeyValueScanner} on top of the specified {@link IndexFileScanner}
   * @param hfs HFile scanner
   */
  public StoreFileIndexScanner(IndexReader reader, IndexFileScanner hfs) {
    this.reader = reader;
    this.ifs = hfs;
  }

  /**
   * Return an array of scanners corresponding to the given set of IndexReaders.
   */
  public static List<StoreFileIndexScanner> getScannersForIndexReaders(
      Collection<IndexReader> fileReaders, boolean cacheBlocks, boolean usePread)
      throws IOException {
    List<StoreFileIndexScanner> scanners = new ArrayList<StoreFileIndexScanner>(fileReaders.size());
    for (IndexReader reader : fileReaders) {
      scanners.add(reader.getStoreFileIndexScanner(cacheBlocks, usePread));
    }
    return scanners;
  }

  /**
   * Return an array of scanners corresponding to the given set of store files.
   */
  public static List<StoreFileIndexScanner> getScannersForStoreFiles(Collection<StoreFile> files,
      boolean cacheBlocks, boolean usePread) throws IOException {
    List<StoreFileIndexScanner> scanners = new ArrayList<StoreFileIndexScanner>(files.size());
    for (StoreFile sf : files) {
      IndexReader ir = sf.getIndexReader();
      if (ir != null) {
        scanners.add(ir.getStoreFileIndexScanner(cacheBlocks, usePread));
      }
    }
    return scanners;
  }

  public String toString() {
    return "StoreFileScanner[" + ifs.toString() + ", cur=" + cur + "]";
  }

  public IndexKeyValue peek() {
    return cur;
  }

  public IndexKeyValue next() throws IOException {
    // winter: one key here!
    IndexKeyValue retKey = cur;
    cur = ifs.getIndexKeyValue();
    try {
      // only seek if we arent at the end. cur == null implies 'end'.
      if (cur != null) ifs.next();
    } catch (IOException e) {
      throw new IOException("Could not iterate " + this, e);
    }
    return retKey;
  }

  public boolean seek(IndexKeyValue key) throws IOException {
    try {
      if (!seekAtOrAfter(ifs, key)) {
        close();
        return false;
      }
      cur = ifs.getIndexKeyValue();
      ifs.next();
      return true;
    } catch (IOException ioe) {
      throw new IOException("Could not seek " + this, ioe);
    }
  }

  public boolean reseek(IndexKeyValue key) throws IOException {
    try {
      if (!reseekAtOrAfter(ifs, key)) {
        close();
        return false;
      }
      cur = ifs.getIndexKeyValue();
      ifs.next();
      return true;
    } catch (IOException ioe) {
      throw new IOException("Could not seek " + this, ioe);
    }
  }

  public void close() {
    // Nothing to close on HFileScanner?
    cur = null;
  }

  /**
   * @param s
   * @param k
   * @return
   * @throws IOException
   */
  public static boolean seekAtOrAfter(IndexFileScanner s, IndexKeyValue k) throws IOException {
    int result = s.seekTo(k.getBuffer(), k.getKeyOffset(), k.getKeyLength());
    if (result < 0) {
      // Passed KV is smaller than first KV in file, work from start of file
      return s.seekTo();
    } else if (result > 0) {
      // Passed KV is larger than current KV in file, if there is a next
      // it is the "after", if not then this scanner is done.
      return s.next();
    }
    // Seeked to the exact key
    return true;
  }

  static boolean reseekAtOrAfter(IndexFileScanner s, IndexKeyValue k) throws IOException {
    // This function is similar to seekAtOrAfter function
    int result = s.reseekTo(k.getBuffer(), k.getKeyOffset(), k.getKeyLength());
    if (result <= 0) {
      return true;
    } else {
      // passed KV is larger than current KV in file, if there is a next
      // it is after, if not then this scanner is done.
      return s.next();
    }
  }

  // StoreFile filter hook.
  public boolean shouldSeek(Scan scan, final SortedSet<byte[]> columns) {
    return reader.shouldSeek(scan, columns);
  }

  @Override
  public long getSequenceID() {
    return reader.getSequenceID();
  }
}
