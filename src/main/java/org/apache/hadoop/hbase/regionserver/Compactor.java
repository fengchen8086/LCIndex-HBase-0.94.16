/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseFileSystem;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.IndexColumnDescriptor;
import org.apache.hadoop.hbase.index.regionserver.IndexKeyValue;
import org.apache.hadoop.hbase.index.regionserver.IndexKeyValueHeap;
import org.apache.hadoop.hbase.index.regionserver.IndexReader;
import org.apache.hadoop.hbase.index.regionserver.IndexWriter;
import org.apache.hadoop.hbase.index.regionserver.StoreFileIndexScanner;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFileWriterV2;
import org.apache.hadoop.hbase.regionserver.compactions.CompactSelection;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import doTestAid.WinterOptimizer;
import doWork.LCCIndexConstant;
import doWork.LCCIndexGenerator;
import doWork.file.LCIndexWriter;
import doWork.jobs.CompactJobQueue;
import doWork.jobs.CompactJobQueue.CompactJob;
import doWork.jobs.CompactJobQueue.NormalCompactJob;
import doWork.jobs.CompactJobQueue.RebuildCompactJob;

/**
 * Compact passed set of files. Create an instance and then call {@ink #compact(Store, Collection,
 * boolean, long)}.
 */
@InterfaceAudience.Private
public class Compactor extends Configured {
  private static final Log LOG = LogFactory.getLog(Compactor.class);
  private CompactionProgress progress;

  Compactor(final Configuration c) {
    super(c);
  }

  /**
   * Compact a list of files for testing. Creates a fake {@link CompactionRequest} to pass to the
   * actual compaction method
   * @param store store which should be compacted
   * @param conf configuration to use when generating the compaction selection
   * @param filesToCompact the files to compact. They are used a the compaction selection for the
   *          generated {@link CompactionRequest}
   * @param isMajor <tt>true</tt> to initiate a major compaction (prune all deletes, max versions,
   *          etc)
   * @param maxId maximum sequenceID == the last key of all files in the compaction
   * @return product of the compaction or null if all cells expired or deleted and nothing made it
   *         through the compaction.
   * @throws IOException
   */
  public StoreFile.Writer compactForTesting(final Store store, Configuration conf,
      final Collection<StoreFile> filesToCompact, boolean isMajor, long maxId) throws IOException {
    return compact(CompactionRequest.getRequestForTesting(store, conf, filesToCompact, isMajor),
      maxId);
  }

  // winter modified for lccindex
  public StoreFile.Writer lcIdxCompact(CompactionRequest request, long maxId, Path lccIndexPath,
      boolean isLocal) throws IOException {
    // Calculate maximum key count after compaction (for blooms)
    // Also calculate earliest put timestamp if major compaction
    int maxKeyCount = 0;
    long earliestPutTs = HConstants.LATEST_TIMESTAMP;
    long maxMVCCReadpoint = 0;

    if (lccIndexPath == null) {
      throw new IOException("winter cannot believe this lccIndexPath be null");
    }

    // pull out the interesting things from the CR for ease later
    final Store store = request.getStore();
    final boolean majorCompaction = request.isMajor();
    final List<StoreFile> filesToCompact = request.getFiles();

    for (StoreFile file : filesToCompact) {
      StoreFile.Reader r = file.getReader();
      if (r == null) {
        LOG.warn("Null reader for " + file.getPath());
        continue;
      }
      // NOTE: getFilterEntries could cause under-sized blooms if the user
      // switches bloom type (e.g. from ROW to ROWCOL)
      long keyCount =
          (r.getBloomFilterType() == store.getFamily().getBloomFilterType()) ? r.getFilterEntries()
              : r.getEntries();
      maxKeyCount += keyCount;
      // Calculate the maximum MVCC readpoint used in any of the involved files
      Map<byte[], byte[]> fileInfo = r.loadFileInfo();
      byte[] tmp = fileInfo.get(HFileWriterV2.MAX_MEMSTORE_TS_KEY);
      if (tmp != null) {
        maxMVCCReadpoint = Math.max(maxMVCCReadpoint, Bytes.toLong(tmp));
      }
      // For major compactions calculate the earliest put timestamp
      // of all involved storefiles. This is used to remove
      // family delete marker during the compaction.
      if (majorCompaction) {
        tmp = fileInfo.get(StoreFile.EARLIEST_PUT_TS);
        if (tmp == null) {
          // There's a file with no information, must be an old one
          // assume we have very old puts
          earliestPutTs = HConstants.OLDEST_TIMESTAMP;
        } else {
          earliestPutTs = Math.min(earliestPutTs, Bytes.toLong(tmp));
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Compacting " + file + ", keycount=" + keyCount + ", bloomtype="
            + r.getBloomFilterType().toString() + ", size="
            + StringUtils.humanReadableInt(r.length()) + ", encoding="
            + r.getHFileReader().getEncodingOnDisk()
            + (majorCompaction ? ", earliestPutTs=" + earliestPutTs : ""));
      }
    }

    // keep track of compaction progress
    this.progress = new CompactionProgress(maxKeyCount);
    // Get some configs
    int compactionKVMax = getConf().getInt("hbase.hstore.compaction.kv.max", 10);
    Compression.Algorithm compression = store.getFamily().getCompression();
    // Avoid overriding compression setting for major compactions if the user
    // has not specified it separately
    Compression.Algorithm compactionCompression =
        (store.getFamily().getCompactionCompression() != Compression.Algorithm.NONE) ? store
            .getFamily().getCompactionCompression() : compression;

    // For each file, obtain a scanner:
    List<StoreFileScanner> scanners =
        StoreFileScanner.getScannersForStoreFiles(filesToCompact, false, false, true);

    // Make the instantiation lazy in case compaction produces no product; i.e.
    // where all source cells are expired or deleted.
    StoreFile.Writer writer = null;
    // Find the smallest read point across all the Scanners.
    long smallestReadPoint = store.getHRegion().getSmallestReadPoint();
    MultiVersionConsistencyControl.setThreadReadPoint(smallestReadPoint);
    try {
      InternalScanner scanner = null;
      try {
        if (store.getHRegion().getCoprocessorHost() != null) {
          scanner =
              store
                  .getHRegion()
                  .getCoprocessorHost()
                  .preCompactScannerOpen(store, scanners,
                    majorCompaction ? ScanType.MAJOR_COMPACT : ScanType.MINOR_COMPACT,
                    earliestPutTs, request);
        }
        if (scanner == null) {
          Scan scan = new Scan();
          scan.setMaxVersions(store.getFamily().getMaxVersions());
          /* Include deletes, unless we are doing a major compaction */
          scanner =
              new StoreScanner(store, store.getScanInfo(), scan, scanners,
                  majorCompaction ? ScanType.MAJOR_COMPACT : ScanType.MINOR_COMPACT,
                  smallestReadPoint, earliestPutTs);
        }
        if (store.getHRegion().getCoprocessorHost() != null) {
          InternalScanner cpScanner =
              store.getHRegion().getCoprocessorHost().preCompact(store, scanner, request);
          // NULL scanner returned from coprocessor hooks means skip normal processing
          if (cpScanner == null) {
            return null;
          }
          scanner = cpScanner;
        }

        int bytesWritten = 0;
        // since scanner.next() can return 'false' but still be delivering data,
        // we have to use a do/while loop.
        List<KeyValue> kvs = new ArrayList<KeyValue>();
        // Limit to "hbase.hstore.compaction.kv.max" (default 10) to avoid OOME
        boolean hasMore;
        do {
          hasMore = scanner.next(kvs, compactionKVMax);
          // Create the writer even if no kv(Empty store file is also ok),
          // because we need record the max seq id for the store file, see
          // HBASE-6059
          if (writer == null) {
            if (lccIndexPath != null) {
              if (isLocal) {
                writer =
                    store.mWinterCreateWriterInLocalTmp(maxKeyCount, compactionCompression, true,
                      maxMVCCReadpoint >= smallestReadPoint, lccIndexPath);
              } else {
                writer =
                    store.mWinterCreateWriterInTmp(maxKeyCount, compactionCompression, true,
                      maxMVCCReadpoint >= smallestReadPoint, lccIndexPath);
              }
            } else {
              writer =
                  store.createWriterInTmp(maxKeyCount, compactionCompression, true,
                    maxMVCCReadpoint >= smallestReadPoint);
            }
          }
          if (writer != null) {
            // output to writer:
            for (KeyValue kv : kvs) {
              if (kv.getMemstoreTS() <= smallestReadPoint) {
                kv.setMemstoreTS(0);
              }
              writer.append(kv);
              // update progress per key
              ++progress.currentCompactedKVs;

              // will not give up, never!
              // check periodically to see if a system stop is requested
              // if (Store.closeCheckInterval > 0) {
              // bytesWritten += kv.getLength();
              // if (bytesWritten > Store.closeCheckInterval) {
              // bytesWritten = 0;
              // isInterrupted(store, writer);
              // }
              // }
            }
          }
          kvs.clear();
        } while (hasMore);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
    } finally {
      if (writer != null) {
        writer.appendMetadata(maxId, majorCompaction);
        writer.close();
      }
    }
    return writer;
  }

  /**
   * Do a minor/major compaction on an explicit set of storefiles from a Store.
   * @param request the requested compaction that contains all necessary information to complete the
   *          compaction (i.e. the store, the files, etc.)
   * @return Product of compaction or null if all cells expired or deleted and nothing made it
   *         through the compaction.
   * @throws IOException
   */
  StoreFile.Writer compact(CompactionRequest request, long maxId) throws IOException {
    // Calculate maximum key count after compaction (for blooms)
    // Also calculate earliest put timestamp if major compaction
    int maxKeyCount = 0;
    long earliestPutTs = HConstants.LATEST_TIMESTAMP;
    long maxMVCCReadpoint = 0;

    // pull out the interesting things from the CR for ease later
    final Store store = request.getStore();
    final boolean majorCompaction = request.isMajor();
    final List<StoreFile> filesToCompact = request.getFiles();

    for (StoreFile file : filesToCompact) {
      StoreFile.Reader r = file.getReader();
      if (r == null) {
        LOG.warn("Null reader for " + file.getPath());
        continue;
      }
      // NOTE: getFilterEntries could cause under-sized blooms if the user
      // switches bloom type (e.g. from ROW to ROWCOL)
      long keyCount =
          (r.getBloomFilterType() == store.getFamily().getBloomFilterType()) ? r.getFilterEntries()
              : r.getEntries();
      maxKeyCount += keyCount;
      // Calculate the maximum MVCC readpoint used in any of the involved files
      Map<byte[], byte[]> fileInfo = r.loadFileInfo();
      byte[] tmp = fileInfo.get(HFileWriterV2.MAX_MEMSTORE_TS_KEY);
      if (tmp != null) {
        maxMVCCReadpoint = Math.max(maxMVCCReadpoint, Bytes.toLong(tmp));
      }
      // For major compactions calculate the earliest put timestamp
      // of all involved storefiles. This is used to remove
      // family delete marker during the compaction.
      if (majorCompaction) {
        tmp = fileInfo.get(StoreFile.EARLIEST_PUT_TS);
        if (tmp == null) {
          // There's a file with no information, must be an old one
          // assume we have very old puts
          earliestPutTs = HConstants.OLDEST_TIMESTAMP;
        } else {
          earliestPutTs = Math.min(earliestPutTs, Bytes.toLong(tmp));
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Compacting " + file + ", keycount=" + keyCount + ", bloomtype="
            + r.getBloomFilterType().toString() + ", size="
            + StringUtils.humanReadableInt(r.length()) + ", encoding="
            + r.getHFileReader().getEncodingOnDisk()
            + (majorCompaction ? ", earliestPutTs=" + earliestPutTs : ""));
      }
    }

    // keep track of compaction progress
    this.progress = new CompactionProgress(maxKeyCount);
    // Get some configs
    int compactionKVMax = getConf().getInt("hbase.hstore.compaction.kv.max", 10);
    Compression.Algorithm compression = store.getFamily().getCompression();
    // Avoid overriding compression setting for major compactions if the user
    // has not specified it separately
    Compression.Algorithm compactionCompression =
        (store.getFamily().getCompactionCompression() != Compression.Algorithm.NONE) ? store
            .getFamily().getCompactionCompression() : compression;

    // For each file, obtain a scanner:
    List<StoreFileScanner> scanners =
        StoreFileScanner.getScannersForStoreFiles(filesToCompact, false, false, true);

    // Make the instantiation lazy in case compaction produces no product; i.e.
    // where all source cells are expired or deleted.
    StoreFile.Writer writer = null;
    // Find the smallest read point across all the Scanners.
    long smallestReadPoint = store.getHRegion().getSmallestReadPoint();
    MultiVersionConsistencyControl.setThreadReadPoint(smallestReadPoint);
    try {
      InternalScanner scanner = null;
      try {
        if (store.getHRegion().getCoprocessorHost() != null) {
          scanner =
              store
                  .getHRegion()
                  .getCoprocessorHost()
                  .preCompactScannerOpen(store, scanners,
                    majorCompaction ? ScanType.MAJOR_COMPACT : ScanType.MINOR_COMPACT,
                    earliestPutTs, request);
        }
        if (scanner == null) {
          Scan scan = new Scan();
          scan.setMaxVersions(store.getFamily().getMaxVersions());
          /* Include deletes, unless we are doing a major compaction */
          scanner =
              new StoreScanner(store, store.getScanInfo(), scan, scanners,
                  majorCompaction ? ScanType.MAJOR_COMPACT : ScanType.MINOR_COMPACT,
                  smallestReadPoint, earliestPutTs);
        }
        if (store.getHRegion().getCoprocessorHost() != null) {
          InternalScanner cpScanner =
              store.getHRegion().getCoprocessorHost().preCompact(store, scanner, request);
          // NULL scanner returned from coprocessor hooks means skip normal processing
          if (cpScanner == null) {
            return null;
          }
          scanner = cpScanner;
        }

        int bytesWritten = 0;
        // since scanner.next() can return 'false' but still be delivering data,
        // we have to use a do/while loop.
        List<KeyValue> kvs = new ArrayList<KeyValue>();
        // Limit to "hbase.hstore.compaction.kv.max" (default 10) to avoid OOME
        boolean hasMore;
        do {
          hasMore = scanner.next(kvs, compactionKVMax);
          // Create the writer even if no kv(Empty store file is also ok),
          // because we need record the max seq id for the store file, see
          // HBASE-6059
          if (writer == null) {
            writer =
                store.createWriterInTmp(maxKeyCount, compactionCompression, true,
                  maxMVCCReadpoint >= smallestReadPoint);
          }
          if (writer != null) {
            // output to writer:
            for (KeyValue kv : kvs) {
              if (kv.getMemstoreTS() <= smallestReadPoint) {
                kv.setMemstoreTS(0);
              }
              writer.append(kv);
              // update progress per key
              ++progress.currentCompactedKVs;

              // check periodically to see if a system stop is requested
              if (Store.closeCheckInterval > 0) {
                bytesWritten += kv.getLength();
                if (bytesWritten > Store.closeCheckInterval) {
                  bytesWritten = 0;
                  isInterrupted(store, writer);
                }
              }
            }
          }
          kvs.clear();
        } while (hasMore);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
    } finally {
      if (writer != null) {
        writer.appendMetadata(maxId, majorCompaction);
        writer.close();
      }
    }
    return writer;
  }

  /**
   * Do a minor/major compaction for store files' index.
   * @param filesToCompact which files to compact
   * @param hfilePath if null, compact index from this file, else compact each StoreFile's index
   *          together
   * @return Product of compaction or null if there is no index column cell
   * @throws IOException
   */
  Path compactIndex(final CompactionRequest request, final Path hfilePath) throws IOException {
    List<StoreFile> filesToCompact = request.getFiles();
    boolean majorCompact = request.isMajor();
    Store store = request.getStore();
    Compression.Algorithm compactionCompression =
        (store.getFamily().getCompactionCompression() != Compression.Algorithm.NONE) ? store
            .getFamily().getCompactionCompression() : store.getFamily().getCompression();
    IndexWriter writer = null;
    List<IndexReader> indexFilesToCompact = new ArrayList<IndexReader>();

    if (majorCompact) { // winter when the storefile path is xx.xx, it is always a major compact
      if (LOG.isDebugEnabled()) {
        LOG.debug("Generate intermediate index file from major compaction file=" + hfilePath
            + " in cf=" + request.getStore().toString());
      }

      StoreFile compactedStoreFile =
          new StoreFile(store.fs, hfilePath, store.conf, store.cacheConf, store.getFamily()
              .getBloomFilterType(), store.getDataBlockEncoder());
      store.passSchemaMetricsTo(compactedStoreFile);
      store.passSchemaMetricsTo(compactedStoreFile.createReader());

      StoreFileScanner compactedFileScanner =
          compactedStoreFile.getReader().getStoreFileScanner(false, false);
      List<Path> flushedIndexFiles = new ArrayList<Path>();

      TreeSet<IndexKeyValue> indexCache = new TreeSet<IndexKeyValue>(store.indexComparator);
      long cacheSize = 0;

      int bytesRead = 0;
      KeyValue kv = null;
      IndexKeyValue ikv = null;

      try {
        compactedFileScanner.seek(KeyValue.createFirstOnRow(HConstants.EMPTY_BYTE_ARRAY));
        while ((kv = compactedFileScanner.next()) != null || !indexCache.isEmpty()) {
          if (kv != null && store.indexMap.containsKey(kv.getQualifier())) {
            ikv = new IndexKeyValue(kv);
            indexCache.add(ikv);
            cacheSize += ikv.heapSize();
          }

          // flush cache to file
          if (cacheSize >= store.indexCompactCacheMaxSize || kv == null) {
            try {
              writer = store.createIndexWriterInLocalTmp(compactionCompression);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Flush intermediate index cache to file=" + writer.getPath()
                    + ", cacheSize=" + StringUtils.humanReadableInt(cacheSize) + ", entries="
                    + indexCache.size() + ", in cf=" + store.toString());
              }
              for (IndexKeyValue tmp : indexCache) {
                writer.append(tmp);
              }
            } finally {
              if (writer != null) {
                writer.close();
                flushedIndexFiles.add(writer.getPath());
              }
              writer = null;
              indexCache.clear();
              cacheSize = 0L;
            }
          }

          // check periodically to see if a system stop is
          // requested
          if (Store.closeCheckInterval > 0 && kv != null) {
            bytesRead += kv.getLength();
            if (bytesRead > Store.closeCheckInterval) {
              bytesRead = 0;
              if (!store.getHRegion().areWritesEnabled()) {
                for (Path indexPath : flushedIndexFiles) {
                  store.localfs.delete(indexPath, false);
                }
                indexCache.clear();
                throw new InterruptedIOException("Aborting compaction index of store " + this
                    + " in region " + store.getHRegion() + " because user requested stop.");
              }
            }
          }
        }
      } finally {
        if (compactedFileScanner != null) compactedFileScanner.close();
      }

      if (flushedIndexFiles.isEmpty()) {
        return null;
      } else if (flushedIndexFiles.size() == 1) {
        Path desPath = store.getIndexFilePathFromHFilePathInTmp(hfilePath);
        store.fs.copyFromLocalFile(true, flushedIndexFiles.get(0), desPath);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Completed index compaction of 1 file, new file=" + flushedIndexFiles.get(0)
              + " in cf=" + store.toString());
        }
        return desPath;
      } else {
        for (Path indexFile : flushedIndexFiles) {
          indexFilesToCompact
              .add(StoreFile.createIndexReader(store.localfs, indexFile, store.conf));
        }
      }
    } else { // else of isMajor
      for (StoreFile tmpfile : filesToCompact) {
        if (tmpfile.getIndexReader() != null) {
          indexFilesToCompact.add(tmpfile.getIndexReader());
        } else {
          System.out.println("winter compactIRIndex compactor do not have index reader!");
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Compact index from " + indexFilesToCompact.size() + " file(s) in cf="
          + store.toString());
    }

    if (!indexFilesToCompact.isEmpty()) {
      List<StoreFileIndexScanner> indexFileScanners =
          StoreFileIndexScanner.getScannersForIndexReaders(indexFilesToCompact, false, false);
      IndexKeyValue startIKV = new IndexKeyValue(null, null, null);
      for (StoreFileIndexScanner scanner : indexFileScanners) {
        scanner.seek(startIKV);
      }

      IndexKeyValueHeap indexFileHeap =
          new IndexKeyValueHeap(indexFileScanners, store.indexComparator);
      IndexKeyValue ikv = null;
      writer = null;

      try {
        while ((ikv = indexFileHeap.next()) != null) {
          if (writer == null) {
            writer = store.createIndexWriterInTmp(compactionCompression);
          }
          writer.append(ikv);
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Completed index compaction of " + indexFilesToCompact.size()
              + " file(s), new file=" + (writer == null ? null : writer.getPath()) + " in cf="
              + store.toString());
        }

      } finally {
        if (writer != null) {
          writer.close();
        }
        if (indexFileScanners != null) {
          indexFileHeap.close();
        }
      }
    }

    // delete all intermediate index file when do a major compaction
    if (majorCompact && indexFilesToCompact != null) {
      for (IndexReader isf : indexFilesToCompact) {
        HBaseFileSystem.deleteFileFromFileSystem(store.localfs, isf.getPath());
      }
    }

    if (writer != null) {
      HBaseFileSystem.renameDirForFileSystem(store.fs, writer.getPath(),
        store.getIndexFilePathFromHFilePathInTmp(hfilePath));
      return store.getIndexFilePathFromHFilePathInTmp(hfilePath);
    }

    return null;
  }

  // handle major and minor compact seperately
  Path mWinterCompactLCCIndex(final CompactionRequest request, final Path hfilePath)
      throws IOException {
    Store store = request.getStore();
    boolean majorCompact = request.isMajor();
    // winter check dot file!

    if (majorCompact) { // winter here will return or throw out IOException
      StoreFile compactedStoreFile =
          new StoreFile(store.fs, hfilePath, store.conf, store.cacheConf, store.getFamily()
              .getBloomFilterType(), store.getDataBlockEncoder());
      store.passSchemaMetricsTo(compactedStoreFile);
      store.passSchemaMetricsTo(compactedStoreFile.createReader());
      StoreFileScanner compactedFileScanner =
          compactedStoreFile.getReader().getStoreFileScanner(false, false);
      IndexColumnDescriptor indexFamily = new IndexColumnDescriptor(store.getFamily());
      assert indexFamily.hasIndex()
          && indexFamily.getIndexType() == IndexColumnDescriptor.IndexType.LCCIndex;
      TreeMap<byte[], DataType> lccIndexQualifierType = indexFamily.mWinterGetQualifierType();
      String desStr =
          store.getHRegion().getTableDesc().getValue(LCCIndexConstant.LC_TABLE_DESC_RANGE_STR);
      LCCIndexGenerator lccindexGenerator = new LCCIndexGenerator(lccIndexQualifierType, desStr);
      KeyValue kv = null;
      List<Path> flushedIndexFiles = new ArrayList<Path>();
      // StoreFile.Writer lccIndexWriter = null;
      LCIndexWriter lccWriterV2 = null;
      try {
        compactedFileScanner.seek(KeyValue.createFirstOnRow(HConstants.EMPTY_BYTE_ARRAY));
        while ((kv = compactedFileScanner.next()) != null) {
          // winter should consider cache size, but not now
          // winter see store.closeCheckInterval, important but optimize it later
          lccindexGenerator.processKeyValue(kv);
        }
        try {
          // flush new!
          KeyValue[] lccIndexResultArray = lccindexGenerator.generatedSortedKeyValueArray();
          if (lccIndexResultArray != null && lccIndexResultArray.length > 0) {
            lccWriterV2 =
                new LCIndexWriter(store, hfilePath, lccindexGenerator.getResultStatistic(),
                    lccindexGenerator.getLCRangeStatistic(), null);
            // lccIndexWriter = store.mWinterCreateWriterInTmp(lccIndexResultArray.length,
            // hfilePath);
            for (KeyValue lcckv : lccIndexResultArray) {
              // System.out.println("winter checking compact for values: "
              // + LCCIndexConstant.mWinterToPrint(lcckv));
              // lccIndexWriter.append(lcckv);
              lccWriterV2.append(lcckv);
            }
          } else {
            System.out.println("winter CompactLCCIndex the lccResults is zero, how come?!");
            throw new IOException("winter CompactLCCIndex the lccResults is zero, how come?!");
          }
        } finally {
          if (lccWriterV2 != null) {
            lccWriterV2.close();
            flushedIndexFiles.add(lccWriterV2.getBaseDirPath());
          }
          lccindexGenerator.clear();
        }
      } finally {
        if (compactedFileScanner != null) compactedFileScanner.close();
      }
      if (flushedIndexFiles.isEmpty()) {
        WinterOptimizer.NeedImplementation("winter CompactLCCIndex but flushedIndexFiles.size = 0");
        throw new IOException("winter CompactLCCIndex but flushedIndexFiles.size = 0");
        // return null;
      } else if (flushedIndexFiles.size() == 1) {
        Path desPath = store.mWinterGetLCCIndexFilePathFromHFilePathInTmp(hfilePath);
        return desPath;
      } else { // winter other sceneries not handled yet
        WinterOptimizer
            .NeedImplementation("winter CompactLCCIndex other sceneries not handled yet");
        throw new IOException("winter CompactLCCIndex other sceneries not handled yet");
      }
    } else { // else of isMajor
      // hfilePath = /hbase/lccAAA/.tmp/B-target
      // lccIndexDirPath = /hbase/lccAAA/.tmp/B-target.lccindex
      // this is safe because lccIndexDir is saved in HDFS
      FileStatus[] fileStatusList = store.fs.listStatus(store.lccIndexDir);
      if (fileStatusList == null || fileStatusList.length == 0) {
        WinterOptimizer.NeedImplementation("winter how could it be, no index file under: "
            + store.lccIndexDir);
        throw new IOException("winter how could it be, no index file under: " + store.lccIndexDir);
      }
      Path targetLCCIndexDirPath = store.mWinterGetLCCIndexFilePathFromHFilePathInTmp(hfilePath);
      if (!store.fs.exists(targetLCCIndexDirPath)) {
        System.out.println("winter target minor compact dir not exists, create: "
            + targetLCCIndexDirPath);
        if (!store.fs.mkdirs(targetLCCIndexDirPath)) {
          WinterOptimizer.NeedImplementation("winter error in mkdir: " + targetLCCIndexDirPath);
          throw new IOException("winter error in mkdir: " + targetLCCIndexDirPath);
        }
      }
      // every qualifier must be merged seperately!
      // merge B1-Q1, B2-Q1, B3-Q1 into B-target-Q1
      for (FileStatus fileStatus : fileStatusList) {
        List<Path> statPaths = new ArrayList<Path>();
        // fileStatus = /hbase/lcc/AAA/f/.lccindex/Q1-...-Q4
        // merge all Bi-Q1, i is changing
        List<StoreFile> lccIndexFilesToCompact = new ArrayList<StoreFile>();
        for (StoreFile lccToBeCompacted : request.getFiles()) {
          // lccToBeCompacted = /hbase/lcc/AAA/f/B1-B3, AAA is always the same
          // existingLCCIndexPath = /hbase/lcc/AAA/f/.lccindex/Q1/Bi. In this loop, Bi is changing
          Path existingLCCIndexPath =
              new Path(fileStatus.getPath(), lccToBeCompacted.getPath().getName());
          if (store.fs.exists(existingLCCIndexPath)) {
            statPaths.add(new Path(fileStatus.getPath(), lccToBeCompacted.getPath().getName()
                + LCCIndexConstant.LC_STAT_FILE_SUFFIX));
            StoreFile tempLCCStoreFile =
                new StoreFile(store.fs, existingLCCIndexPath, store.conf, store.cacheConf, store
                    .getFamily().getBloomFilterType(), store.getDataBlockEncoder());
            tempLCCStoreFile.createReader();
            lccIndexFilesToCompact.add(tempLCCStoreFile);
          } else {
            System.out.println("winter CompactLCCIndex error, lccindex should exist but not: "
                + existingLCCIndexPath);
            throw new IOException("winter error, lccindex should exist but not: "
                + existingLCCIndexPath);
          }
        }
        CompactSelection lccIndexFilesToCompactCS =
            new CompactSelection(store.conf, lccIndexFilesToCompact);
        CompactionRequest lccCR =
            new CompactionRequest(request.getHRegion(), store, lccIndexFilesToCompactCS,
                request.isMajor(), request.getPriority());
        long maxId = StoreFile.getMaxSequenceIdInList(lccIndexFilesToCompact, true);
        // targetLCCIndexDirPath = /hbase/lccAAA/.tmp/B-target.lccindex
        // writer to tempFileLocation, targetPath = /hbase/lcc/AAA/.tmp/BBB.lccindex/Q1-Q4
        Path targetDirPath = new Path(targetLCCIndexDirPath, fileStatus.getPath().getName());
        mWinterCompactStatFile(store.localfs, statPaths, new Path(targetLCCIndexDirPath, fileStatus
            .getPath().getName() + LCCIndexConstant.LC_STAT_FILE_SUFFIX));
        lcIdxCompact(lccCR, maxId, targetDirPath, false);
        for (StoreFile sf : lccIndexFilesToCompact) {
          sf.closeReader(true);
        }
        // System.out.println("winter minor compact flush to: " + lccCompactWriter.getPath());
      }
      return targetLCCIndexDirPath;
    }
  }

  // handle major and minor compact seperately
  CompactJob mWinterCompactLCCIndexLocal(final CompactionRequest request, final Path writtenPath)
      throws IOException {
    // check reference file, not supported yet!
    boolean needToRebuild = false;
    for (StoreFile sf : request.getFiles()) {
      if (sf.getPath().getName().indexOf(".") != -1 || sf.isReference()) {
        needToRebuild = true;
        break;
      }
    }
    CompactJob job = null;
    if (needToRebuild) {
      job = new RebuildCompactJob(request.getStore(), request, writtenPath);
    } else {
      job = new NormalCompactJob(request.getStore(), request, writtenPath);
    }
    CompactJobQueue.getInstance().addJob(job);
    return job;
  }

  public void mWinterCompactStatFile(FileSystem fs, List<Path> statPaths, Path targetPath)
      throws IOException {
    if (statPaths.size() == 0) return;
    // System.out.println("winter compact stat to file -->> " + targetPath);
    // for (Path p : statPaths) {
    // System.out.println("winter mWinterCompactStatFile: " + p);
    // }
    BufferedReader[] readers = new BufferedReader[statPaths.size()];
    for (int i = 0; i < readers.length; ++i) {
      readers[i] = new BufferedReader(new InputStreamReader(fs.open(statPaths.get(i))));
    }
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(targetPath, true)));
    long temp = 0;
    String line = null;
    while (true) {
      line = readers[0].readLine();
      if (line == null) {
        break;
      }
      temp = Long.parseLong(line);
      for (int i = 1; i < readers.length; ++i) {
        line = readers[i].readLine();
        temp += Long.parseLong(line);
      }
      bw.write(temp + "\n");
    }
    for (BufferedReader br : readers) {
      br.close();
    }
    bw.close();
  }

  void isInterrupted(final Store store, final StoreFile.Writer writer) throws IOException {
    if (store.getHRegion().areWritesEnabled()) return;
    // Else cleanup.
    writer.close();
    store.getFileSystem().delete(writer.getPath(), false);
    throw new InterruptedIOException("Aborting compaction of store " + store + " in region "
        + store.getHRegion() + " because user requested stop.");
  }

  CompactionProgress getProgress() {
    return this.progress;
  }
}
