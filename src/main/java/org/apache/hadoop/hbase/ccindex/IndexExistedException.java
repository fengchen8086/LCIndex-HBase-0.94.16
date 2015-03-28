package org.apache.hadoop.hbase.ccindex;

/**
 * Thrown when add an index on a column which has an index already.
 * 
 * @author wanhao
 *
 */
public class IndexExistedException extends Exception {

	private static final long serialVersionUID = 6200178126806286870L;

	/** default constructor */
	public IndexExistedException() {
		super();
	}

	public IndexExistedException(String s) {
		super(s);
	}
}