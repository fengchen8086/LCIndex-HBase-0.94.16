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

forceflush=true
maxRecordNumber=10000
threadNumber=12
cacheSize=1000
regionNumber=100

rangeFilePrefix=`pwd`/filter
remoteLogDir=`pwd`/all-logs
commonHBaseHome=`pwd`
generatedDataPath=`pwd`/datatpc.dat
tpchFilePath="/home/fengchen/softwares/tpch_2_17_0/dbgen/orders.tbl"
statFile=`pwd`/stat.dat

putClassName="tpch.put.TPCHPutMain"
scanClassName="tpch.scan.TPCHScanMain"
generateDataClassName="tpch.put.TPCHDataGenerator"
JVM_PARAM='-Xmx1000m -XX:+UseConcMarkSweepGC'

fun_TestPut(){
	java $JVM_PARAM -cp $run_classpath $putClassName $hbase_conf_filepath $winter_assigned_filepath $1 $2 $3 $4 $5 $6 $7
}

fun_TestScan(){
	java $JVM_PARAM -cp $run_classpath $scanClassName $hbase_conf_filepath $winter_assigned_filepath $1 $2 $3 
}

fun_GenerateData(){
	java $JVM_PARAM -cp $run_classpath $generateDataClassName $1 $2 $3 $4 $5
}

fun_RunInsertData(){
	fun_TestPut $1 $2 $forceflush $generatedDataPath $3 $4 $5
}


fun_CopyAndClearLogs(){
	nowDat=`date +%Y%m%d-%H%M%S`
	targetDir=$remoteLogDir/$nowDat-$1
        mkdir $targetDir
	for i in `cat ~/allnodes`; do
		scp -q hec-$i:$commonHBaseHome/logs/* $targetDir
		for j in `ssh hec-$i ls $commonHBaseHome/logs`; do
			ssh hec-$i "cat /dev/null > `pwd`/logs/$j"
		done
		echo hec-$i done
	done
}

fun_RunScanData(){
	fun_TestScan $1 ${rangeFilePrefix}"-01" $2
	fun_TestScan $1 ${rangeFilePrefix}"-02" $2
	fun_TestScan $1 ${rangeFilePrefix}"-03" $2
}

#currentTestName="hbase"
#currentTestName="cm"
#currentTestName="cc"
#currentTestName="ir"
currentTestName="lcc"

fun_GenerateData $maxRecordNumber $tpchFilePath $generatedDataPath $statFile $threadNumber
fun_RunInsertData $currentTestName $maxRecordNumber $threadNumber $statFile $regionNumber
fun_RunScanData $currentTestName $cacheSize

#fun_CopyAndClearLogs $currentTestName


