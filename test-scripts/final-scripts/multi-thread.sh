#!/bin/sh

run_classpath=".:a.jar:hbase-0.94.16.jar"
cd lib
out=`ls *.jar`
for i in $out; do
    run_classpath=${run_classpath}:`pwd`/$i;
done

cd ../

hbase_conf_filepath=`pwd`"/conf/hbase-site.xml"
echo $hbase_conf_filepath
winter_assigned_filepath=`pwd`/conf/winter-assign
echo $winter_assigned_filepath

forceflush=true
recordNumber=40
# 1k->300K data, 1yi=30G data, winter should use 5Yi in actually
# when memstore flush = 10M, 35000 records incures one flush
# now memstore flush = 10M, 3500 records incures one flush
threadNumber=4
putClassName="doMultiple.put.MultiPutMain"
scanClassName="doTest.scan.ScanMain"
generateDataClassName="doMultiple.GenerateDataIntoFileMultiple"
checkResultsDir=`pwd`"/temp/lcc/check-results"
generatedDataPath=`pwd`"/datafile.dat"
JVM_PARAM='-Xmx1000m -XX:+UseConcMarkSweepGC'

fun_TestPut(){
    java $JVM_PARAM -cp $run_classpath $putClassName $hbase_conf_filepath $winter_assigned_filepath $1 $2 $3 $4 $5
}

fun_TestScan(){
    java $JVM_PARAM -cp $run_classpath $scanClassName $hbase_conf_filepath $winter_assigned_filepath $1 $2 $3 $4 $5
}

fun_GenerateData(){
    java $JVM_PARAM -cp $run_classpath $generateDataClassName $1 $2 $3
}

#currentTestName="hbase"
#currentTestName="cm"
#currentTestName="cc"
#currentTestName="ir"
currentTestName="lcc"

fun_RunInsertData(){
    fun_TestPut $currentTestName $recordNumber $forceflush $generatedDataPath $1
}

fun_RunScanData(){
    fun_TestScan $currentTestName $recordNumber 0.0001 $checkResultsDir
    fun_TestScan $currentTestName $recordNumber 0.001 $checkResultsDir
    fun_TestScan $currentTestName $recordNumber 0.01 $checkResultsDir
    fun_TestScan $currentTestName $recordNumber 0.1 $checkResultsDir
    fun_TestScan $currentTestName $recordNumber 1 $checkResultsDir
}

java $JVM_PARAM -cp $run_classpath doTestAid.Nothing

#fun_GenerateData $recordNumber $generatedDataPath $threadNumber
#fun_RunInsertData $threadNumber
#sleep 10
#fun_RunScanData

