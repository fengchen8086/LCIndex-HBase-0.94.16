#!/bin/sh

run_classpath=".:a.jar:hbase-0.94.16.jar"
cd lib
out=`ls *.jar`
for i in $out; do
    run_classpath=${run_classpath}:`pwd`/$i;
done
cd ../

startClientScript="test-client-start.sh"

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
generatedDataDir=`pwd`/data
tpchFilePath="/home/fengchen/softwares/tpch_2_17_0/dbgen/orders.tbl"
statFileName=stat.dat
clientFile=`pwd`/clients
testLog=`pwd`/remoteLog
mkdir $testLog

remoteServerClass="tpch.remotePut.TPCHRemoteServer"
JVM_PARAM='-Xmx1000m -XX:+UseConcMarkSweepGC'

workPath=`pwd`
serverHost=`hostname`

# workloadType totalRecordNum data_input_dir stat_name client_hosts_file each_client_thread_number forceflush
echo "start server"

nohup java $JVM_PARAM -cp $run_classpath $remoteServerClass $1 $2 $3 $4 $5 $6 $7 > $testLog/server.log 2>&1 &

sleep 3

# start clients
for c in `cat $clientFile`; do
	echo $c;
	scp -q $startClientScript $c:$workPath
	scp -q a.jar $c:$workPath
#		echo $serverHost $c $j
	#	nohup "ssh $c cd $workPath && $startClientScript $serverHost $c $j" &
	ssh $c "cd $workPath && nohup sh $startClientScript $serverHost $c $6 &"
done 

