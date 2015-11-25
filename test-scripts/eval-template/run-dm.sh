#!/usr/bin/env bash

start_dstat(){
   for host in `cat slaves`;do
      ssh $host "rm -r `pwd`/tmp; mkdir -p `pwd`/tmp"
      ssh $host "nohup dstat --time --cpu --mem --disk --net --output `pwd`/tmp/DSTAT_${host}.csv > /dev/null" &> /dev/null &
   done 
}

stop_dstat(){
   DSTAT_LOG=`pwd`/result/dstat
   rm $DSTAT_LOG -r
   mkdir -p $DSTAT_LOG

   for host in `cat slaves`;do
      ssh $host "sudo killall python -s INT"
      scp -r -q $host:`pwd`/tmp/* $DSTAT_LOG
   done 
   ./extDstat.py $DSTAT_LOG ${DSTAT_LOG}/../dstat.log
}

clear_cache(){
    echo "clear cache"
    for host in localhost `cat slaves`;do
        ssh -t $host "sudo sync;sudo bash -c 'echo 3 > /proc/sys/vm/drop_caches'" &> /dev/null
    done
}

MPI_D_HOME="/home/nbtest/develop/datampi-debug"
MPI_D_SLAVES=`pwd`/conf/slaves
RESTART_PATH="/mnt/nbtest_data/datampi/checkpoint"
DATAMPI_EXAMPLE_JAR="${MPI_D_HOME}/share/datampi/examples/common/common-example.jar"
HADOOP_HOME="/home/nbtest/develop/hadoop-1.2.1"
HDFS_CONF_CORE="${HADOOP_HOME}/conf/core-site.xml"

SOURCE_PATH=data/10G-text
TARGET_PATH=tera_mpid
MAPS=28
REDS=28

st(){
	cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_EXAMPLE_JAR} mpid.examples.SortOnHDFSDataLocal \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"
}

tst(){
	cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_EXAMPLE_JAR} mpid.examples.TeraSortOnHDFSDataLocal \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"
}

wc()
{
	cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A 1 \
        -jar ${DATAMPI_EXAMPLE_JAR} mpid.examples.WordCountOnHDFSDataLocal \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"
}


run(){
	task=${SOURCE_PATH#*\/}-dm
    wc
    hadoop fs -rmr $TARGET_PATH && sleep 5
    clear_cache
    start_dstat
    t1=`date +%s`
    echo $cmd && ($cmd) 2>&1 | tee ${task}_run.log
    t2=`date +%s`
    stop_dstat
	
    echo $((t2 - t1)) | tee -a ${task}_run.log
}

SOURCE_PATH=data/10G-text
run
