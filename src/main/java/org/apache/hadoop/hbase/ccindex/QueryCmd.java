package org.apache.hadoop.hbase.ccindex;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * Index query tool, if you want to set query arguments, use this tool instead of JDBC.
 * 
 * @author wanhao
 *
 */
public class QueryCmd {
	
	public static void main(String[] args) throws Exception {
		
		Options options = new Options();
		options.addOption("s", "sql", true, "sql query sentence, e.g. select * from t");
		options.addOption("i", "useIndex", true, "set it true if you want to use index when scanning, default is true.");
		options.addOption("g", "maxGetsPerScanner", true, "Set max get threads for a scanner, it will be used when SecIndex or ImpSecIndex is chose to scan and index table doesn't contain all result columns, default value is 10.");
		options.addOption("t", "maxScanners", true, "max number of threads to start to query, default value is 10.");
		options.addOption("p", "printResults", true, "set it true if you want to print result, default value is false.");
		options.addOption("n", "printInterval", true, "print progress interval, default is 100 rows.");
		options.addOption("c", "scannerCaching", true, "Sets the number of rows that a scanner will fetch at once, default is 'hbase.client.scanner.caching'.");
		
		try{
			CommandLine cli = new GnuParser().parse(options, args);
			
			String sql=cli.getOptionValue("s");
			IndexQuerySQL query=new IndexQuerySQL(sql);
			
			IndexTable table=new IndexTable(query.getTableName());
			if(cli.hasOption("i")){
				table.setUseIndex(Boolean.parseBoolean(cli.getOptionValue("i")));
			}
			
			if(cli.hasOption("g")){
				table.setMaxGetsPerScan(Integer.parseInt(cli.getOptionValue("g")));
			}
			
			if(cli.hasOption("t")){
				table.setMaxScanThreads(Integer.parseInt(cli.getOptionValue("t")));
			}
			
			if(cli.hasOption("c")){
				table.setScannerCaching(Integer.parseInt(cli.getOptionValue("c")));
			}
			
			boolean print=false;
			if(cli.hasOption("p")){
				print=Boolean.parseBoolean(cli.getOptionValue("p"));
			}

			IndexResultScanner rs=table.getScanner(query);
			Result r;
			int interval=100;
			
			if(cli.hasOption("n")){
				interval=Integer.parseInt(cli.getOptionValue("n"));
			}
			Map<byte[], DataType> columnMap=table.getColumnInfoMap();
			
			while((r=rs.next())!=null){
				if(print){
					printLine(r, columnMap);
				}
				if(rs.getTookOutCount()%interval==0){
					String str = "------------results:" + rs.getTotalCount() + ","
                            + "time:" + rs.getTotalScanTime()+ "ms," 
							+ "totalscanner:" + rs.getTotalScannerNum() + "," 
                            + "finishedscanner:" + rs.getFinishedScannerNum()+"-------------";
					System.out.println(str);
				}
			}
	
			String str = "------------results:" + rs.getTotalCount() + ","
                    + "time:" + rs.getTotalScanTime()+ "ms," 
					+ "totalscanner:" + rs.getTotalScannerNum() + "," 
                    + "finishedscanner:" + rs.getFinishedScannerNum()+"-------------final";
			System.out.println(str);
		
		}catch(Exception e){
			e.printStackTrace();
			new HelpFormatter().printHelp("query cmd", options);
		}
	}
	
	private static void printLine(Result r, Map<byte[], DataType> columnInfoMap){
		StringBuilder sb=new StringBuilder();
		sb.append("row="+ Bytes.toString(r.getRow()));
		byte[] tmp=null;
		for(KeyValue kv:r.list()){
			tmp = KeyValue.makeColumn(kv.getFamily(), kv.getQualifier());
			sb.append(","+Bytes.toString(tmp) + "=");
			
			if(columnInfoMap == null || columnInfoMap.isEmpty()){
				sb.append(Bytes.toString(kv.getValue()));
			}else{
				switch(columnInfoMap.get(tmp)){
					case DOUBLE:
						sb.append(Bytes.toDouble(kv.getValue()));
						break;
					case LONG:
						sb.append(Bytes.toLong(kv.getValue()));
						break;
					case INT:
						sb.append(Bytes.toInt(kv.getValue()));
						break;
					case STRING:
						sb.append(Bytes.toString(kv.getValue()));
						break;
					default:
						sb.append(Bytes.toString(kv.getValue()));
						break;
				}
			}
		}
		
		System.out.println(sb.toString());
	}

}





