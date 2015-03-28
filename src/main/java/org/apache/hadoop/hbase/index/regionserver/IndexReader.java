package org.apache.hadoop.hbase.index.regionserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.regionserver.BlockCache;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;

import org.apache.hadoop.io.RawComparator;

public class IndexReader {
  static final Log LOG = LogFactory.getLog(Reader.class.getName());

  private final IndexFile.Reader reader;
  protected long sequenceID = -1;

  public IndexReader(FileSystem fs, Path path, BlockCache blockCache, boolean inMemory)
      throws IOException {
    reader = new IndexFile.Reader(fs, path, blockCache, inMemory);
  }

  public IndexFile.Reader getIndexFileReader(){
    return this.reader;
  }
  
  public Path getPath() {
    return new Path(reader.getName());
  }

  public RawComparator<byte[]> getComparator() {
    return reader.getComparator();
  }

  /**
   * Get a scanner to scan over this StoreFile.
   * @param cacheBlocks should this scanner cache blocks?
   * @param pread use pread (for highly concurrent small readers)
   * @return a scanner
   */
  public StoreFileIndexScanner getStoreFileIndexScanner(boolean cacheBlocks, boolean pread) {
    return new StoreFileIndexScanner(this, getIndexFileScanner(cacheBlocks, pread));
  }

  /**
   * Warning: Do not write further code which depends on this call. Instead use
   * getStoreFileScanner() which uses the StoreFileScanner class/interface which is the preferred
   * way to scan a store with higher level concepts.
   * @param cacheBlocks should we cache the blocks?
   * @param pread use pread (for concurrent small readers)
   * @return the underlying IndexFileScanner
   */
  public IndexFileScanner getIndexFileScanner(boolean cacheBlocks, boolean pread) {
    return reader.getScanner(cacheBlocks, pread);
  }

  public void close() throws IOException {
    reader.close();
  }

  public boolean shouldSeek(Scan scan, final SortedSet<byte[]> columns) {
    return true;
  }

  public Map<byte[], byte[]> loadFileInfo() throws IOException {
    Map<byte[], byte[]> fi = reader.loadFileInfo();

    return fi;
  }

  public int getFilterEntries() {
    return reader.getFilterEntries();
  }

  public ByteBuffer getMetaBlock(String bloomFilterDataKey, boolean cacheBlock) throws IOException {
    return reader.getMetaBlock(bloomFilterDataKey, cacheBlock);
  }

  public byte[] getLastKey() {
    return reader.getLastKey();
  }

  public byte[] midkey() throws IOException {
    return reader.midkey();
  }

  public long length() {
    return reader.length();
  }

  public int getEntries() {
    return reader.getEntries();
  }

  public byte[] getFirstKey() {
    return reader.getFirstKey();
  }

  public long indexSize() {
    return reader.indexSize();
  }

  public long getSequenceID() {
    return sequenceID;
  }

  public void setSequenceID(long sequenceID) {
    this.sequenceID = sequenceID;
  }
}
