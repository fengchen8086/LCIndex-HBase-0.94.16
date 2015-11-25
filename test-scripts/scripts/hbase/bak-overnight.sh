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
# 1k->300K data, 1yi=30G data, winter should use 5Yi in actually
# when memstore flush = 10M, 35000 records incures one flush
# now memstore flush = 10M, 3500 records incures one flush
threadNumber=3
putClassName="doMultiple.put.MultiPutMain"
scanClassName="doTest.scan.ScanMain"
generateDataClassName="doMultiple.GenerateDataIntoFileMultiple"
checkResultsDir=`pwd`"/temp/lcc/check-results"
generatedDataPath=`pwd`"/datafile.dat"

fun_TestPut(){
    java -cp $run_classpath $putClassName $hbase_conf_filepath $winter_assigned_filepath $1 $2 $3 $4 $5
}

fun_TestScan(){
    java -cp $run_classpath $scanClassName $hbase_conf_filepath $winter_assigned_filepath $1 $2 $3 $4
}

fun_GenerateData(){
    java -cp $run_classpath $generateDataClassName $1 $2 $3
}

#currentTestName="hbase"
#currentTestName="cm"
#currentTestName="cc"
#currentTestName="ir"
currentTestName="lcc"

fun_RunInsertData(){
    fun_TestPut $1 $2 $3 $4 $5
#    fun_TestPut $currentTestName $recordNumber $forceflush $generatedDataPath $1
}

fun_RunScanData(){
#    fun_TestScan $currentTestName $recordNumber 0.001 $checkResultsDir
    fun_TestScan $1 $2 0.0001 $checkResultsDir
    sleep 5
    fun_TestScan $1 $2 0.001 $checkResultsDir
    sleep 5
    fun_TestScan $1 $2 0.01 $checkResultsDir
    sleep 5
    fun_TestScan $1 $2 0.1 $checkResultsDir
    sleep 5
    fun_TestScan $1 $2 1 $checkResultsDir
}

fun_RestartHBase(){
#    stop-hbase.sh
    ./kill-regionservers.sh
    ./delete-all.sh
    ./clear-logs.sh 
    sleep 10
    start-hbase.sh
}

recordNumber=0
thisFileName=""
saveDir=`pwd`/night-run

for i in 40 60; do 
    recordNumber=$(($i * 10000))
    for type in hbase cm cc ir lcc; do
        echo $type-$recordNumber
	for tn in 4 8 10 12 16; do
            threadNumber=$tn
            thisFileName=$saveDir/$type-$recordNumber-$threadNumber
            fun_GenerateData $recordNumber $generatedDataPath $threadNumber > $thisFileName
            fun_RunInsertData $type $recordNumber $forceflush $generatedDataPath $threadNumber >> $thisFileName
            sleep 20
            fun_RunScanData $type $recordNumber >> $thisFileName
            fun_RestartHBase
            sleep 60
        done
    done
done

