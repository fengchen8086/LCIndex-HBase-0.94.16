package doTest.put;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ccindex.CCIndexAdmin;
import org.apache.hadoop.hbase.ccindex.IndexExistedException;
import org.apache.hadoop.hbase.ccindex.IndexSpecification;
import org.apache.hadoop.hbase.ccindex.IndexSpecification.IndexType;
import org.apache.hadoop.hbase.ccindex.IndexTable;
import org.apache.hadoop.hbase.ccindex.IndexTableDescriptor;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.util.Bytes;

import doTest.put.PutTestConstants.CF_INFO;

public class PutCCIndex extends ClassPutBase {

	CCIndexAdmin indexAdmin;

	public PutCCIndex(String confPath, String newAddedFile, String tableName,
			int recordNumber, boolean forceFlush) throws IOException {
		super(confPath, newAddedFile, tableName, recordNumber, forceFlush);
		indexAdmin = new CCIndexAdmin(admin);
	}

	@Override
	protected void checkTable() throws IOException {
		if (indexAdmin.tableExists(tableName)) {
			System.out.println("coffey PutCCIndex deleting existing table: "
					+ tableName);
			indexAdmin.disableTable(tableName);
			indexAdmin.deleteTable(tableName);
		}
		System.out.println("coffey ccindex creating  table: " + tableName);
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		List<CF_INFO> cfs = PutTestConstants.getCFInfo();
		List<IndexSpecification> indexList = new ArrayList<IndexSpecification>();
		Map<byte[], DataType> map = new TreeMap<byte[], DataType>(
				Bytes.BYTES_COMPARATOR);
		for (CF_INFO ci : cfs) {
			if (ci.isIndex) {
				IndexSpecification index = new IndexSpecification(
						Bytes.toBytes(FAMILY_NAME + ":" + ci.qualifier),
						IndexType.CCINDEX);
				indexList.add(index);
				System.out.println("coffey ccindex has index on cf: "
						+ ci.qualifier + ", type is: " + ci.type);
			}
			map.put(Bytes.toBytes(FAMILY_NAME + ":"
					+ Bytes.toBytes(ci.qualifier)), ci.type);
		}
		tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes(FAMILY_NAME)));
		IndexTableDescriptor indexDesc;
		try {
			indexDesc = new IndexTableDescriptor(tableDesc,
					indexList.toArray(new IndexSpecification[indexList.size()]));
			indexAdmin.createTable(indexDesc);
		} catch (IndexExistedException e) {
			e.printStackTrace();
		}
		System.out.println("coffey ccindex creating table: " + tableName
				+ " finish");
	}

	@Override
	public void insertData() throws IOException {
		IndexTable indexTable = new IndexTable(conf, tableName);
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		int counter = 0, doneSize = 0;
		while (!queue.isEmpty()) {
			indexTable.put(queue.poll(), false);
			if (counter == PRINT_INTERVAL) {
				counter = 0;
				System.out.println("coffey insert data " + doneSize
						+ " class: " + this.getClass().getName() + ", time: "
						+ dateFormat.format(new Date()));
			}
			++counter;
			++doneSize;
		}
		indexTable.close();
		System.out.println("coffey totally insert " + doneSize + " records");
	}

	@Override
	public void finish() throws IOException, InterruptedException {
		if (forceFlush) {
			indexAdmin.flushAll(Bytes.toBytes(tableName));
		}
	}
}
