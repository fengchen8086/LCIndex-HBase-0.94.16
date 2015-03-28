package org.apache.hadoop.hbase.ccindex;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * IndexDelete holds a delete for main data table and relevant deletes for index tables.
 * 
 * @author wanhao
 *
 */
public class IndexDelete {
	private Map<byte[],Delete> deletes;
	
	public IndexDelete(){
		deletes=new TreeMap<byte[],Delete>(Bytes.BYTES_COMPARATOR);
	}
	
	public void addDelete(byte[] index,Delete delete){
		deletes.put(index, delete);
	}
	
	public Map<byte[],Delete>  getDeletes(){
		return deletes;
	}
	
	public void setWAL(Durability wal){
	  for(Delete delete:deletes.values()){
	    delete.setDurability(wal);
	  }
	}
}
