package org.apache.hadoop.hbase.ccindex;


import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Parse, optimize and store a SQl query sentence.
 * 
 * @author wanhao
 */

public class IndexQuerySQL {
	// columns that are supposed to be in the query result
	private byte[][] resultColumns = null;

	// name of the main data table
	private byte[] tableName = null;

	// original query conditions
	String queryCondition = null;
	
	// optimized query condition stored as Range array
	private Range[][] ranges = null;
	
	//column info of this table, mainly each column's type info
	private Map<byte[], DataType> columnInfoMap=null;
	
	private byte[] startKey = null;

	private byte[] endKey = null;
	
	/**
	 * Get columns' info of this table, mainly column's data type info.
	 * 
	 * @return
	 */
	public Map<byte[], DataType> getColumnInfo(){
		return this.columnInfoMap;
	}
	
	/**
	 * Set columns' info of this table, mainly column's data type info.
	 */
	public void setColumnInfo(Map<byte[], DataType> columnInfoMap){
		this.columnInfoMap=columnInfoMap;
	}
	
	public void setStartKey(byte[] startKey){
		this.startKey = startKey;
	}
	
	public void setEndKey(byte[] endKey){
		this.endKey = endKey;
	}
	
	/**
	 * Get the optimized query conditions stored in {@code Range} array.
	 * 
	 * @return
	 * @throws Exception 
	 */
	public Range[][] getRanges() throws Exception{
		if(this.ranges==null){
			parseQuery();
		}
		return this.ranges;
	}
	
	/**
	 * Get the table name in the sql sentence.
	 * 
	 * @return
	 */
	public byte[] getTableName(){
		return this.tableName;
	}
	
	/**
	 * Get result columns in sql sentence.
	 * 
	 * @return resultColumns-null if no column is specified, else the specified columns
	 */
	public byte[][] getResultColumn(){
		return this.resultColumns;
	}

	/**
	 * Construct from a sql sentence, such as "select * from table where f1:c1<'10' and f2:c1>'20'".
	 * At the same time, specify column info and parse query conditions.
	 * 
	 * @param querySql - sql query sentence
	 * @throws Exception 
	 */
	public IndexQuerySQL(String querySql, Map<byte[], DataType> columnInfoMap) throws Exception {
		this(querySql);
		this.columnInfoMap=columnInfoMap;
		this.parseQuery();
	}
	
	/**
	 * Construct from a sql sentence, such as "select * from table where f1:c1<'10' and f2:c1>'20'".
	 * 
	 * @param querySql - sql query sentence
	 */
	public IndexQuerySQL(String querySql) {
		// check parameters passed from main function
		querySql = querySql.replace(",", " ").trim();
		String[] tempArray = querySql.split("[\\s]+");

		// check keyword "select"
		if (tempArray[0].compareToIgnoreCase("select") != 0) {
			throw new IllegalArgumentException("Keyword 'select' or 'SELECT' are not found!");
		}

		// get the position of keyword "from"
		int fromFlag = 0;
		for (fromFlag = 1; fromFlag < tempArray.length; fromFlag++) {
			if (tempArray[fromFlag].compareToIgnoreCase("from") == 0)
				break;
		}
		if (fromFlag >= tempArray.length) {
			throw new IllegalArgumentException("Keywords 'from' or 'FROM' are not found!");
		}
		
		// get selected columns contained in query result
		String[] tempColumns = new String[fromFlag - 1];
		for (int i = 1; i < fromFlag; i++) {
			tempColumns[i - 1] = tempArray[i];
		}
		
		if (tempColumns.length == 1 && tempColumns[0].equals("*")) {
			resultColumns=null;
		} else {
			resultColumns=Bytes.toByteArrays(tempColumns);
		}

		// get table name
		tableName = Bytes.toBytes(tempArray[fromFlag + 1]);

		// check keyword "where"
		if (fromFlag + 2 < tempArray.length) {
			if(tempArray[fromFlag + 2].compareToIgnoreCase("where") != 0) {
				throw new IllegalArgumentException(
						"Keyword 'where' or 'WHERE' are not found or in the right position!");
			}
			// get query condition
			queryCondition = "";
			for (int i = fromFlag + 3; i < tempArray.length; i++) {
				queryCondition += tempArray[i] + " ";
			}
		}else{
			queryCondition=null;
		}
		
	}
	
	private void parseQuery() throws Exception{

//TODO uncomment this in new version
//		if(columnInfo==null ||columnInfo.size()==0){
//			throw new IllegalArgumentException("Column Info is not set! Please specify column info before query!");
//		}
		
		//optimize the query condition
		if(queryCondition!=null){			
			
//			ranges = scanOptimize.optimizeQuery(queryCondition, columnInfoMap); 
		}else{
			ranges=new Range[1][1];
			ranges[0][0]=new Range(IndexConstants.KEY,this.getTableName());
			ranges[0][0].setStartType(CompareOp.GREATER_OR_EQUAL);
			if(this.startKey!=null){
				ranges[0][0].setStartValue(this.startKey);
			}else{
				ranges[0][0].setStartValue(HConstants.EMPTY_BYTE_ARRAY);
			}
			if(this.endKey!=null){
				ranges[0][0].setEndType(CompareOp.LESS_OR_EQUAL);
				ranges[0][0].setEndValue(endKey);
			}
		}

//		ranges = new Range[range.length - 1][];
//
//		// set base columns of every query range
//		for (int i = 0; i < range.length - 1; i++) {
//			ranges[i] = new Range[range[i].length];
//			for (int j = 0; j < range[i].length; j++) {
//				ranges[i][j] = range[i][j];
//			}
//		}
	}

	/**
	 * Construct an IndexSQL directly from given parameters.
	 * 
	 * @param tableName
	 * @param resultColumns
	 * @param ranges
	 */
	public IndexQuerySQL(byte[] tableName, byte[][] resultColumns, Range[][] ranges){
		if(tableName==null){
			throw new IllegalArgumentException("Table name is null!");
		}
		this.tableName=tableName;
		
		if(ranges==null || ranges.length==0){
			throw new IllegalArgumentException("Range array is null!");
		}
		
		for(int i=0;i<ranges.length;i++){
			if(ranges[i]==null || ranges[i].length==0){
				throw new IllegalArgumentException("Range["+i+"] is null!");
			}
			for(int j=0;j<ranges[i].length;j++){
				if(ranges[i][j]==null){
					throw new IllegalArgumentException("Range["+i+"]["+j+"] is null!");
				}
			}
		}
		
		this.ranges=ranges;
		
		if(resultColumns==null ||resultColumns.length==0){
			this.resultColumns=null;
		}else{
			this.resultColumns=resultColumns;
		}
	}

	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append("SELECT ");
		if(resultColumns==null){
			sb.append("*");
		}else{
			for(int i=0;i<resultColumns.length;i++){
				sb.append(Bytes.toString(resultColumns[i]));
				if(i!=resultColumns.length-1){
					sb.append(",");
				}
			}
		}
		sb.append(" FROM ");
		sb.append(Bytes.toString(tableName));
		sb.append(" WHERE ");

		int i = 0, j = 0;
		for (Range[] r : ranges) {
			j = 0;
			sb.append("( ");
			for (Range ran : r) {
				sb.append(ran.toString());
				if (j != r.length - 1){
					sb.append(" AND ");
				}
				j++;
			}
			sb.append(" )");
			if (i != ranges.length - 1){
				sb.append(" OR ");
			}
			i++;
		}

		return sb.toString();
	}
}
