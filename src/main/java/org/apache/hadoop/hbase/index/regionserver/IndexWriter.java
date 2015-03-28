package org.apache.hadoop.hbase.index.regionserver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.index.regionserver.IndexKeyValue.IndexKVComparator;
import org.apache.hadoop.hbase.io.hfile.Compression;

public class IndexWriter {
  protected IndexFile.Writer writer;

  /**
   * Creates an IndexFile.Writer that also write helpful meta data.
   * @param fs file system to write to
   * @param path file name to create
   * @param blocksize HDFS block size
   * @param compress HDFS block compression
   * @param conf user configuration
   * @param comparator key comparator
   * @param bloomType bloom filter setting
   * @param maxKeys maximum amount of keys to add (for blooms)
   * @throws IOException problem writing to FS
   */
  public IndexWriter(FileSystem fs, Path path, short replication, int blocksize, Compression.Algorithm compress,
      final Configuration conf, final IndexKVComparator comparator) throws IOException {
    writer = new IndexFile.Writer(fs, path, replication, blocksize, compress, comparator.getRawComparator());
  }

  public void append(final IndexKeyValue kv) throws IOException {
    writer.append(kv);
  }

  public Path getPath() {
    return this.writer.getPath();
  }

  public void append(final byte[] key, final byte[] value) throws IOException {
    writer.append(key, value);
  }

  public void close() throws IOException {
    writer.close();
  }

  public void appendFileInfo(byte[] key, byte[] value) throws IOException {
    writer.appendFileInfo(key, value);
  }
}
