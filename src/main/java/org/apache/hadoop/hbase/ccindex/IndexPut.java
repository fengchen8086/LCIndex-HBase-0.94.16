package org.apache.hadoop.hbase.ccindex;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * IndexPut holds a put for main data table and relevant puts for index tables.
 * 
 * @author wanhao
 *
 */
public class IndexPut {
	private Map<byte[], Put> puts;

	public IndexPut() {
		puts = new TreeMap<byte[], Put>(Bytes.BYTES_COMPARATOR);
	}

	public void addPut(byte[] index, Put put) {
		puts.put(index, put);
	}

	public Map<byte[], Put> getPuts() {
		return puts;
	}

	public void setWAL(Durability wal) {
		for (Put put : puts.values()) {
			put.setDurability(wal);
		}
	}
}
