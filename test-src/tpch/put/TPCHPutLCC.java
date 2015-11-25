package tpch.put;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.index.client.IndexColumnDescriptor;
import org.apache.hadoop.hbase.index.client.IndexDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import tpch.put.TPCHConstants.TPCH_CF_INFO;
import doWork.LCCIndexConstant;
import doWork.file.LCCHFileMoverClient;

public class TPCHPutLCC extends TPCHPutBaseClass {

  public TPCHPutLCC(String confPath, String newAddedFile, String tableName, int recordNumber,
      boolean forceFlush, String loadDataPath, int threadNum, String statFile, int regionNumbers)
      throws IOException {
    super(confPath, newAddedFile, tableName, recordNumber, forceFlush, loadDataPath, threadNum,
        statFile, regionNumbers);
  }

  @Override
  protected void checkTable() throws IOException {
    if (admin.tableExists(tableName)) {
      String localPath = conf.get(LCCIndexConstant.LCINDEX_LOCAL_DIR);
      if (localPath != null) {
        String tableDirName = localPath + "/" + tableName;
        File dir = new File(tableDirName);
        if (dir.exists()) {
          FileUtils.deleteDirectory(dir);
        }
        String hostnames = conf.get(LCCIndexConstant.LCINDEX_REGIONSERVER_HOSTNAMES);
        String parts[] = hostnames.split(LCCIndexConstant.LCINDEX_REGIONSERVER_HOSTNAMES_DELIMITER);
        for (String hostname : parts) {
          LCCHFileMoverClient cli = new LCCHFileMoverClient(hostname, conf);
          cli.deleteRemoteFile(tableDirName);
        }
      }
      System.out.println("coffey lcc deleting existing table: " + tableName);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    System.out.println("coffey lcc creating lcc table: " + tableName);
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    IndexColumnDescriptor family = new IndexColumnDescriptor(FAMILY_NAME, 2);
    List<TPCH_CF_INFO> cfs = TPCHConstants.getCFInfo();
    for (TPCH_CF_INFO ci : cfs) {
      if (ci.isIndex) {
        IndexDescriptor index = new IndexDescriptor(Bytes.toBytes(ci.qualifier), ci.type);
        family.addIndex(index);
        System.out.println("coffey lcc has lcc index on cf: " + ci.qualifier + ", type is: "
            + ci.type);
      }
    }
    tableDesc.addFamily(family);
    fillRangeFromFile(tableDesc);
    admin.createTable(tableDesc, splitKeys);
    System.out.println("coffey lcc creating lcc table: " + tableName + " finish");
  }

  private void fillRangeFromFile(HTableDescriptor tableDesc) throws IOException {
    File file = new File(statFile);
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    StringBuilder sb = new StringBuilder();
    while ((line = br.readLine()) != null) {
      if (line.startsWith(LCCIndexConstant.ROWKEY_RANGE)) continue;
      sb.append(line).append(LCCIndexConstant.LCC_TABLE_DESC_RANGE_DELIMITER);
    }
    br.close();
    tableDesc.setValue(LCCIndexConstant.LC_TABLE_DESC_RANGE_STR, sb.toString());
  }
}
