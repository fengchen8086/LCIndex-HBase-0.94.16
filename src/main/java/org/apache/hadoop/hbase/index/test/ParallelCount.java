package org.apache.hadoop.hbase.index.test;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Count a table with parallel threads.
 * 
 * @author wanhao
 *
 */
public class ParallelCount {
	public static void main(String[] args) throws IOException {
		String tableName=null;
		
		Options options = new Options();
		options.addOption("t", "tableName", true, "name of table which you want to count");
		options.addOption("n", "threadsNum", true, "max count threads running at the same time, default is 10");
		
		try{
			CommandLine cli = new GnuParser().parse(options, args);
			
			if(cli.hasOption("t")){
				tableName=cli.getOptionValue("t");
			}else{
				throw new ParseException("option: -t, --tableName is not specified!");
			}
			
			int maxPoolSize=10;
			if(cli.hasOption("n")){
				maxPoolSize=Integer.valueOf(cli.getOptionValue("n")).intValue();
			}else{
				System.out.println("option: -n, --threadsNum is not specified, default "+maxPoolSize+" is used!");
			}
			
			Configuration conf=HBaseConfiguration.create();
			HTable table=new HTable(conf,tableName);
			HRegionInfo[] regions = table.getRegionsInfo().keySet().toArray(new HRegionInfo[0]);
			CountRunnable[] scanners=new CountRunnable[regions.length];
			
			long startTime=System.currentTimeMillis();
			ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
					maxPoolSize, maxPoolSize, 10, TimeUnit.SECONDS,
					new LinkedBlockingQueue<Runnable>(),
					new ThreadPoolExecutor.CallerRunsPolicy());
			for(int i=0;i<scanners.length;i++){
				scanners[i]=new CountRunnable(tableName,conf,regions[i]);
				threadPool.execute(scanners[i]);
			}
			
			int finished=0;
			int running=0;
			int waiting=0;
			long count=0;
			while(true){
				finished=0;
				running=0;
				waiting=0;
				count=0;
				
				for(int i=0;i<scanners.length;i++){
					if(scanners[i].isFinished()){
						finished++;
					}else if(scanners[i].isRunning()){
						running++;
					}else{
						waiting++;
					}
					count+=scanners[i].getCount();
				}
				
				System.out.println(count+ ", " +(int) (1.0 * count / (1.0 * (System.currentTimeMillis() - startTime) / 1000))
						+ " r/s, finished=" + finished + ", running=" + running + ", waiting=" + waiting);
	
				if(finished<scanners.length){
					Threads.sleep(1000);
				}else{
					break;
				}
			}
			
			threadPool.shutdown();
			long endTime=System.currentTimeMillis();
			
			System.out.println("----------------------------------");
			System.out.println("result="+count+", time="+(endTime-startTime)/1000+" s, speed="+(int)(1.0*count/(1.0*(endTime-startTime)/1000))+" r/s");
			
			System.exit(0);
		}catch(ParseException e){
			System.out.println(e.getMessage());
			new HelpFormatter().printHelp("count", options );
		}
	}
	
	public static class CountRunnable implements Runnable{
		private String tableName=null;
		private Configuration conf=null;
		private HRegionInfo region=null;
		private long count=0;
		private volatile boolean finished=false;
		private volatile boolean running=false;
		
		public CountRunnable(String tableName, Configuration conf, HRegionInfo region){
			this.tableName=tableName;
			this.conf=conf;
			this.region=region;
			this.count=0;
		}
		
		public void run(){
			finished=false;
			running=true;
			try {
				HTable table=new HTable(conf,tableName);
				Scan scan=new Scan();
				scan.setCacheBlocks(false);
				scan.setMaxVersions(1);
				scan.setCaching(1000);
				scan.setStartRow(region.getStartKey());
				scan.setStopRow(region.getEndKey());
				
				FilterList flist=new FilterList();
				flist.addFilter(new KeyOnlyFilter());
				flist.addFilter(new FirstKeyOnlyFilter());
				scan.setFilter(flist);
				
				ResultScanner rs=table.getScanner(scan);
				while((rs.next())!=null){
					count++;
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}finally{
				finished=true;
				running=false;
			}

		}
		
		public long getCount(){
			return count;
		}
		
		public boolean isFinished(){
			return finished;
		}
		
		public boolean isRunning(){
			return running;
		}
	}
}
