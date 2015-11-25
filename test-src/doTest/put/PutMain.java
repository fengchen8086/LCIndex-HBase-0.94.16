package doTest.put;

import java.io.IOException;
import java.util.ArrayList;

public class PutMain {

	int ROUND = 1;

	public void work(String confPath, String assignedFile, String testClass,
			int recordNumber, boolean forceFlush, String loadDataPath)
			throws IOException, InterruptedException {
		double totalTime = 0, temp;
		ArrayList<Double> timeList = new ArrayList<Double>();
		for (int i = 0; i < ROUND; ++i) {
			temp = runOneTime(confPath, assignedFile, testClass, recordNumber,
					forceFlush, loadDataPath);
			totalTime += temp;
			timeList.add(temp);
			if (ROUND > 1) {
				Thread.sleep(PutTestConstants.ROUND_SLEEP_TIME);
			}
		}
		System.out.println("coffey report put, run " + testClass + " for "
				+ recordNumber + " records, have run " + ROUND
				+ " times, avg: " + totalTime / ROUND);
		System.out.println("coffey reporting put each time: ");
		for (int i = 0; i < timeList.size(); ++i) {
			System.out.println("coffey report put round " + i + ": "
					+ timeList.get(i));
		}
	}

	private double runOneTime(String confPath, String assignedFile,
			String testClass, int recordNumber, boolean forceFlush,
			String loadDataPath) throws IOException, InterruptedException {
		ClassPutBase putBase = null;
		if ("HBase".equalsIgnoreCase(testClass)) {
			putBase = new PutHBase(confPath, assignedFile,
					PutTestConstants.HBASE_TABLE_NAME, recordNumber, forceFlush);
		} else if ("IR".equalsIgnoreCase(testClass)) {
			putBase = new PutIR(confPath, assignedFile,
					PutTestConstants.IR_TABLE_NAME, recordNumber, forceFlush);
		} else if ("LCC".equalsIgnoreCase(testClass)) {
			putBase = new PutLCC(confPath, assignedFile,
					PutTestConstants.LCC_TABLE_NAME, recordNumber, forceFlush);
		} else if ("CC".equalsIgnoreCase(testClass)) {
			putBase = new PutCCIndex(confPath, assignedFile,
					PutTestConstants.CCIndex_TABLE_NAME, recordNumber,
					forceFlush);
		} else if ("CM".equalsIgnoreCase(testClass)) {
			putBase = new PutCMIndex(confPath, assignedFile,
					PutTestConstants.CMIndex_TABLE_NAME, recordNumber,
					forceFlush);
		}
		if (putBase == null) {
			System.out.println("coffey test class " + testClass
					+ " not supported!");
		}
		putBase.checkTable();
		putBase.generateData(loadDataPath);
		System.out.println("coffey generate data done, next to insert Data");
		long start = System.currentTimeMillis();
		try {
			putBase.insertData();
		} catch (IOException e) {
			e.printStackTrace();
		}
		putBase.finish();
		long timeCost = System.currentTimeMillis() - start;
		return timeCost / 1000.0;
	}

	public static void usage() {
		System.out
				.println("PutMain configure-file-path user-defined-file testclass(hbase/ir/lcc) recordnumber(int) forceflush(true/false) loaddatapath(optional, generate random data if blank)");
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		if (args.length == 0) {
			new PutMain().work(
					"/home/winter/softwares/hbase-0.94.16/conf/hbase-site.xml",
					"/home/winter/softwares/hbase-0.94.16/conf/winter-assign",
					"ir", 1000, true,
					"/home/winter/softwares/hbase-0.94.16/datafile.dat");
			return;
		}
		int recordNumber = 10;
		boolean forceFlush = false;
		String loadDataPath = null;
		if (args.length < 5) {
			usage();
			return;
		}
		recordNumber = Integer.valueOf(args[3]);
		forceFlush = Boolean.valueOf(args[4]);
		if (args.length >= 6) {
			loadDataPath = args[5];
		}
		new PutMain().work(args[0], args[1], args[2], recordNumber, forceFlush,
				loadDataPath);
	}
}
