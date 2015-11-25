#!/bin/sh

run_classpath=".:a.jar:hbase-0.94.16.jar"
cd lib
out=`ls *.jar`
for i in $out; do
    run_classpath=${run_classpath}:`pwd`/$i;
done

cd ../

hbase_conf_filepath=`pwd`"/conf/hbase-site.xml"
winter_assigned_filepath=`pwd`/conf/winter-assign

forceflush=false
putClassName="tpch.put.TPCHPutMain"
scanClassName="tpch.scan.TPCHScanMain"
generateDataClassName="tpch.put.TPCHDataGenerator"
threadNumber=20
generatedDataPath=`pwd`/datetpc.dat
cacheSize=100
tpchFilePath="/home/fengchen/softwares/tpch_2_17_0/dbgen/orders.tbl"
rangeFilePrefix=`pwd`/filter
maxRecordNumber=10000
remoteLogDir=`pwd`/all-logs

commonHBaseHome=`pwd`

fun_TestPut(){
	java -cp $run_classpath $putClassName $hbase_conf_filepath $winter_assigned_filepath $1 $2 $3 $4 $5
}

fun_TestScan(){
	java -cp $run_classpath $scanClassName $hbase_conf_filepath $winter_assigned_filepath $1 $2 $3 
}

fun_GenerateData(){
	java -cp $run_classpath $generateDataClassName $1 $2 $3 $4
}

fun_RunInsertData(){
	fun_TestPut $1 $2 $forceflush $generatedDataPath $3
}


fun_CopyAndClearLogs(){
	nowDat=`date +%Y%m%d-%H%M%S`
	mkdir $remoteLogDir/$nowDat
	for i in `cat ~/allnodes`; do
		mkdir $remoteLogDir/$nowDat/hec-$i
		scp -q hec-$i:$commonHBaseHome/logs/* $remoteLogDir/$nowDat/hec-$i
		for j in `ssh hec-$i ls $commonHBaseHome/logs`; do
			ssh hec-$i "echo '' > `pwd`/logs/$j"
		done
		echo hec-$i done
	done
}

fun_RunScanData(){
	fun_TestScan $1 ${rangeFilePrefix}"-01" $2
	fun_TestScan $1 ${rangeFilePrefix}"-02" $2
	fun_TestScan $1 ${rangeFilePrefix}"-03" $2
#	fun_TestScan $1 `pwd`/single-filter $2
}

#currentTestName="hbase"
#currentTestName="cm"
currentTestName="cc"
#currentTestName="ir"
#currentTestName="lcc"

#fun_GenerateData $maxRecordNumber $tpchFilePath $generatedDataPath $threadNumber
#fun_RunInsertData $currentTestName $maxRecordNumber $threadNumber 
fun_RunScanData $currentTestName $cacheSize

#fun_CopyAndClearLogs

