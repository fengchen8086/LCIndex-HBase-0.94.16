package org.apache.hadoop.hbase.index.regionserver;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A facade for a {@link org.apache.hadoop.hbase.regionserver.StoreFile.IndexReader} that serves up
 * either the top or bottom half of a IndexFile where 'bottom' is the first half of the file
 * containing the keys that sort lowest and 'top' is the second half of the file with keys that sort
 * greater than those of the bottom half. The top includes the split files midkey, of the key that
 * follows if it does not exist in the file.
 * <p>
 * This type works in tandem with the {@link Reference} type. This class is used reading while
 * Reference is used writing.
 * <p>
 * This file is not splitable. Calls to {@link #midkey()} return null.
 */
public class HalfIndexReader extends IndexReader {
  final Log LOG = LogFactory.getLog(HalfIndexReader.class);
  final boolean top;

  protected final byte[] splitkey;

  public HalfIndexReader(final FileSystem fs, final Path p, final BlockCache c, final Reference r)
      throws IOException {
    super(fs, p, c, false);
    this.splitkey = KeyValue.createKeyValueFromKey(r.getSplitKey()).getRow();
    this.top = Reference.isTopFileRegion(r.getFileRegion());
  }

  public boolean isTop() {
    return this.top;
  }

  @Override
  public IndexFileScanner getIndexFileScanner(final boolean cacheBlocks, final boolean pread) {
    final IndexFileScanner s = super.getIndexFileScanner(cacheBlocks, pread);
    return new IndexFileScanner() {
      final IndexFileScanner delegate = s;

      public ByteBuffer getKey() {
        return delegate.getKey();
      }

      public String getKeyString() {
        return delegate.getKeyString();
      }

      public ByteBuffer getValue() {
        return delegate.getValue();
      }

      public String getValueString() {
        return delegate.getValueString();
      }

      public IndexKeyValue getIndexKeyValue() {
        return delegate.getIndexKeyValue();
      }

      private boolean shouldInclude() {
        ByteBuffer bb = getValue();
        boolean cmp =
            Bytes.compareTo(bb.array(), bb.arrayOffset(), bb.limit(), splitkey, 0, splitkey.length) >= 0;
        if ((cmp && top) || (!cmp && !top)) return true;
        return false;
      }

      public boolean next() throws IOException {
        while (delegate.next()) {
          if (shouldInclude()) return true;
        }
        return false;
      }

      public boolean seekTo() throws IOException {
        if (!this.delegate.seekTo()) return false;
        if (shouldInclude()) return true;
        return this.next();
      }

      public int seekTo(byte[] key) throws IOException {
        return seekTo(key, 0, key.length);
      }

      public int seekTo(byte[] key, int offset, int length) throws IOException {
        int result = delegate.seekTo(key, offset, length);
        if (result == 0) {
          if (!shouldInclude()) return 1; // return 1, so upper caller have to call next() to get
                                          // the right ikv
        }
        return result;
      }

      @Override
      public int reseekTo(byte[] key) throws IOException {
        return reseekTo(key, 0, key.length);
      }

      @Override
      public int reseekTo(byte[] key, int offset, int length) throws IOException {
        int result = delegate.reseekTo(key, offset, length);
        if (result <= 0) {
          if (!shouldInclude()) return 1; // return 1, so upper caller have to call next() to get
                                          // the right ikv
        }
        return result;
      }

      public IndexFile.Reader getReader() {
        return this.delegate.getReader();
      }

      public boolean isSeeked() {
        return this.delegate.isSeeked();
      }
    };
  }

  @Override
  public byte[] getLastKey() {
    return super.getLastKey();
  }

  @Override
  public byte[] midkey() throws IOException {
    // Returns null to indicate file is not splitable.
    return null;
  }
}
