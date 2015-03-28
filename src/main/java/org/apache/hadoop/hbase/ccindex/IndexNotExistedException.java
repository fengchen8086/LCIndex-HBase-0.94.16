package org.apache.hadoop.hbase.ccindex;

/**
 * Thrown when delete an index which is not existed.
 * 
 * @author wanhao
 *
 */
public class IndexNotExistedException extends Exception {

	private static final long serialVersionUID = 1983268999134867928L;

	/** default constructor */
	public IndexNotExistedException() {
		super();
	}

	/**
	 * @param s message
	 */
	public IndexNotExistedException(String s) {
		super(s);
	}
}