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
maxRecordNumber=500000
threadNumber=10
regionNumber=100
cacheSize=1000
deleteIfExists=false

rangeFilePrefix=`pwd`/filter
remoteLogDir=`pwd`/all-logs
commonHBaseHome=`pwd`
generatedDataDir=`pwd`/data
tpchFilePath="/home/fengchen/softwares/tpch_2_17_0/dbgen/orders.tbl"
statFileName=stat.dat
clientFile=`pwd`/clients
dstatGatherPath=`pwd`/gather-dstat
storageGatherPath=`pwd`/gather-storage

mkdir $dstatGatherPath
mkdir $storageGatherPath

remotePutScript="test-start-all.sh"
scanClassName="tpch.scan.TPCHScanMain"
generateDataClassName="tpch.remotePut.TPCHRemoteDataGenerator"
JVM_PARAM='-Xmx1000m -XX:+UseConcMarkSweepGC'

fun_TestRemotePut(){
	# workloadType totalRecordNum data_input_dir stat_name client_hosts_file each_client_thread_number forceflush
	sh $remotePutScript $1 $2 $3 $4 $5 $6 $7
}

fun_TestScan(){
	java $JVM_PARAM -cp $run_classpath $scanClassName $hbase_conf_filepath $winter_assigned_filepath $1 $2 $3 
}

fun_GenerateRemoteData(){
	# max_record_number tpc_data_source data_output_dir stat_name client_hosts_file each_client_thread_number delete-if-exist
	mkdir $3
	java $JVM_PARAM -cp $run_classpath $generateDataClassName $1 $2 $3 $4 $5 $6 $7
}

start_dstat(){
   for host in `cat ~/hbase-nodes`;do
      ssh hec-$host "rm -r `pwd`/tmp; mkdir -p `pwd`/tmp"
      ssh hec-$host "nohup dstat --time --cpu --mem --disk --net --output `pwd`/tmp/DSTAT_${host}.csv > /dev/null" &> /dev/null &
   done
}

stop_dstat(){
   DSTAT_LOG=`pwd`/result/dstat
   rm $DSTAT_LOG -r
   mkdir -p $DSTAT_LOG
   for host in `cat ~/allnodes`;do
      echo "cp data from "$i" to "$1
      scp -r -q hec-$host:`pwd`/tmp/* $DSTAT_LOG
   done
   mv $DSTAT_LOG $1
}

fun_CheckStorage(){
    checkStorageOut=`pwd`/storage-07725162.dat
    > $checkStorageOut
    ssh hec-14 "hadoop fs -dus /hbase/*" | tee -a $checkStorageOut
    for i in `cat ~/hbase-nodes`; do
        echo $i
        echo $i >> $checkStorageOut
        ssh hec-$i "du -s /home/fengchen/data/lccindex" | tee -a $checkStorageOut
    done
    mv $checkStorageOut $1
}


fun_CopyAndClearLogs(){
        targetDir=$remoteLogDir/$2-$1
        mkdir $targetDir
        for i in `cat ~/allnodes`; do
                ssh hec-$i "rm $commonHBaseHome/logs/*.out.*"
                scp -q hec-$i:$commonHBaseHome/logs/* $targetDir
                for j in `ssh hec-$i ls $commonHBaseHome/logs`; do
                        ssh hec-$i "cat /dev/null > `pwd`/logs/$j"
                done
                echo hec-$i done
        done
}

fun_RunScanData(){
	fun_TestScan $1 ${rangeFilePrefix}"-01" $2
	sleep 20
	fun_TestScan $1 ${rangeFilePrefix}"-02" $2
	sleep 20
	fun_TestScan $1 ${rangeFilePrefix}"-03" $2
	sleep 20
	fun_TestScan $1 ${rangeFilePrefix}"-04" $2
	sleep 20
	fun_TestScan $1 ${rangeFilePrefix}"-05" $2
}

fun_RestartHBase(){
    echo "restarting hbase"
    ssh hec-14 "cd /home/fengchen/softwares/hbase-0.94.16 && ./kill-regionservers.sh"
    ssh hec-14 "cd /home/fengchen/softwares/hbase-0.94.16 && ./delete-all.sh"
    ssh hec-14 "cd /home/fengchen/softwares/hbase-0.94.16 && ./clear-zk.sh"
    #./clear-logs.sh 
    # clear HDFS
    ssh hec-14 "cd /home/fengchen/softwares/hadoop-1.0.4 && ./kill-hdfs.sh"
    ssh hec-14 "cd /home/fengchen/softwares/hadoop-1.0.4 && ./delete-hdfs-all.sh"
    ssh hec-14 "cd /home/fengchen/softwares/hadoop-1.0.4 && hadoop namenode -format"
    ssh hec-14 "cd /home/fengchen/softwares/hadoop-1.0.4 && start-dfs.sh"
    sleep 10
    ssh hec-14 "cd /home/fengchen/softwares/hadoop-1.0.4 && hadoop dfsadmin -safemode leave"
    sleep 15
    ssh hec-14 "cd /home/fengchen/softwares/hbase-0.94.16 && ./start-zk.sh"
    ssh hec-14 "cd /home/fengchen/softwares/hbase-0.94.16 && start-hbase.sh"
    echo "restart hbase done"
}


thisFileName=""
saveDir=`pwd`/test-results
mkdir $saveDir

# max_record_number tpc_data_source data_output_dir stat_name client_hosts_file each_client_thread_number delete-if-exist
# workloadType totalRecordNum data_input_dir stat_name client_hosts_file each_client_thread_number forceflush

for i in 500; do 
#for i in 1; do 
    maxRecordNumber=$(($i * 10000))
    for tn in 30; do
	fun_GenerateRemoteData $maxRecordNumber $tpchFilePath $generatedDataDir $statFileName $clientFile $tn $deleteIfExists
        for type in cc; do
	    fun_RestartHBase
            ./kill-tests.sh
            sleep 30
	    nowDate=`date +%Y%m%d-%H%M%S`
            thisFileName=$saveDir/$type-$maxRecordNumber-$tn-$nowDate
            echo "start insert "$type-$maxRecordNumber-$tn", flush to:"$thisFileName 
	    start_dstat
   	    fun_TestRemotePut $type $maxRecordNumber $generatedDataDir $statFileName $clientFile $tn $forceflush 2>&1 > $thisFileName
            echo "finish insert "$type-$maxRecordNumber-$tn
            sleep 180
	    stop_dstat $dstatGatherPath/insert-$type-$maxRecordNumber-$tn-$nowDate
            thisFileName=$saveDir/scan-$type-$maxRecordNumber-$nowDate 
            echo "start scan "$type-$maxRecordNumber", flush to:"$thisFileName
	    start_dstat
            fun_RunScanData $type $cacheSize 2>&1 > $saveDir/scan-$type-$maxRecordNumber-$nowDate
	    stop_dstat $dstatGatherPath/scan-$type-$maxRecordNumber-$nowDate
            fun_CheckStorage $storageGatherPath/$type-$maxRecordNumber-$tn-$nowDate
            echo "finish scan "$type-$maxRecordNumber
            fun_CopyAndClearLogs $type-$maxRecordNumber $nowDate
        done
    done
done

