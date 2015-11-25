#!/bin/sh

run_classpath=".:a.jar:hbase-0.94.16.jar"
cd lib
out=`ls *.jar`
for i in $out; do
    run_classpath=${run_classpath}:`pwd`/$i;
done
cd ../

hbase_conf_filepath=`pwd`"/conf/hbase-site.xml"
assigned_filepath=`pwd`/conf/winter-assign
testLog=`pwd`/remoteLog
mkdir $testLog

serverName=$1
thisHost=$2
threadNum=$3
suffix=$4

clientClassName="tpch.remotePut.TPCHRemoteClient"
JVM_PARAM='-Xmx1000m -XX:+UseConcMarkSweepGC'

fun_StartClient(){
	java $JVM_PARAM -cp $run_classpath $clientClassName $hbase_conf_filepath $assigned_filepath $1 $2 $3
}

#nohup fun_StartClient $serverName $thisHost $thisID > $testLog/$thisHost-$thisID.log 2>&1 &
#fun_StartClient $serverName $thisHost $thisID 2>&1 | tee $testLog/$thisHost-$thisID.log 

echo "stat client $1 $2 $3"
nohup java $JVM_PARAM -cp $run_classpath $clientClassName $hbase_conf_filepath $assigned_filepath $1 $2 $3 > $testLog/$thisHost-$threadNum-$suffix.log 2>&1 &


