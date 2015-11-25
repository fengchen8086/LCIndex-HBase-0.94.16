package tpch.scan;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ccindex.IndexTable;
import org.apache.hadoop.hbase.ccindex.SimpleIndexKeyGenerator;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.util.Bytes;

import tpch.put.TPCHConstants;
import tpch.put.TPCHConstants.TPCH_CF_INFO;
import doWork.LCCIndexConstant;

public class TPCHScanCCIndex extends TPCHScanBaseClass {

	public TPCHScanCCIndex(String confPath, String newAddedFile,
			String tableName, List<Range> ranges) throws IOException {
		super(confPath, newAddedFile, tableName, ranges);
	}

	private DataType mainIndexType;
	private String mainIndexQualifier;

	@Override
	public ResultScanner getScanner() throws IOException {
		IndexTable indexTable = new IndexTable(conf, tableName);
		HTable tableToScan = null;
		Scan scan = new Scan();
		FilterList filters = new FilterList();
		Range bestRange = selectTheBestRange();
		for (Range r : ranges) {
			if (r == bestRange)
				continue;
			if (r.getStartValue() != null) {
				filters.addFilter(new SingleColumnValueFilter(r.getFamily(), r
						.getQualifier(), r.getStartType(), r.getStartValue()));
			}
			if (r.getStopValue() != null) {
				filters.addFilter(new SingleColumnValueFilter(r.getFamily(), r
						.getQualifier(), r.getStopType(), r.getStopValue()));
			}
			System.out.println("coffey cmindex add filter for range: "
					+ Bytes.toString(bestRange.getColumn())
					+ " ["
					+ LCCIndexConstant.getStringOfValueAndType(
							bestRange.getDataType(), bestRange.getStartValue())
					+ ","
					+ LCCIndexConstant.getStringOfValueAndType(
							bestRange.getDataType(), bestRange.getStopValue())
					+ "]");
		}
		tableToScan = indexTable.indexTableMaps.get(bestRange.getColumn());
		mainIndexType = bestRange.getDataType();
		mainIndexQualifier = Bytes.toString(bestRange.getQualifier());
		scan.setStartRow(bestRange.getStartValue());
		scan.setStopRow(bestRange.getStopValue());
		System.out.println("coffey cmindex main index range: "
				+ Bytes.toString(bestRange.getColumn())
				+ " ["
				+ LCCIndexConstant.getStringOfValueAndType(
						bestRange.getDataType(), bestRange.getStartValue())
				+ ","
				+ LCCIndexConstant.getStringOfValueAndType(
						bestRange.getDataType(), bestRange.getStopValue())
				+ "]");
		scan.setCacheBlocks(false);
		scan.setFilter(filters);
		return tableToScan.getScanner(scan);
	}

	@Override
	public void printString(Result result) {
		StringBuilder sb = new StringBuilder();
		List<KeyValue> kv = null;
		SimpleIndexKeyGenerator kg = new SimpleIndexKeyGenerator();
		sb.append("row="
				+ LCCIndexConstant.getStringOfValueAndType(mainIndexType,
						result.getRow()));
		byte[][] bytes = kg.parseIndexRowKey(result.getRow());
		sb.append(", key="
				+ LCCIndexConstant.getStringOfValueAndType(DataType.STRING,
						bytes[0]));
		sb.append(", value="
				+ LCCIndexConstant.getStringOfValueAndType(mainIndexType,
						bytes[1]));

		List<TPCH_CF_INFO> cfs = TPCHConstants.getCFInfo();
		for (TPCH_CF_INFO ci : cfs) {
			if (ci.qualifier.equals(mainIndexQualifier)) {
				continue;
			}
			kv = result.getColumn(Bytes.toBytes(TPCHConstants.FAMILY_NAME),
					Bytes.toBytes(ci.qualifier));
			if (kv.size() != 0) {
				sb.append(", ["
						+ TPCHConstants.FAMILY_NAME
						+ ":"
						+ ci.qualifier
						+ "]="
						+ LCCIndexConstant.getStringOfValueAndType(ci.type,
								(kv.get(0).getValue())));
			}
		}
		System.out.println(sb.toString());
	}
}
