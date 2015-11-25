#!/usr/bin/env bash

[ "$DEFINE_ONERROR" != "true" ] && echo "onerror function doesn't define" && exit 1
[ "$IMPORT_BASIC" = "true" ] && return
[ "$IMPORT_BASIC" != "true" ] && IMPORT_BASIC=true

##  log analysis

get_running_time()
{
    type=$1
    [ "$2" != "" ] && startLine=$2
    [ "$startLine" = "0" -a "$PHRASE_DECLARE" != "" ] && startLine=`grep -n "${PHRASE_DECLARE}" "${LOG_NAME}" | awk -F":" '{print $1}'`
    startLine=$((startLine+1))
    if [ "$type" = "mpid" ]
    then
        tt=`sed -n "${startLine},$"p ${LOG_NAME} | grep "ended, used time" | awk '{print $10}'`
        [ "$tt" = "" ] && tt=0
         echo "$tt sec"
    elif [ "$type" = "had" ]
    then
        tt=`sed -n "${startLine},$"p ${LOG_NAME} | grep "The job took" | awk '{print $4}'`
        [ "$tt" = "" ] && tt=0
         echo "$tt sec"
    fi
}

check_log()
{
    [ "$1" != "" ] && startLine=$1
    [ "$startLine" = "0" -a "$PHRASE_DECLARE" != "" ] && startLine=`grep -n "${PHRASE_DECLARE}" "${LOG_NAME}" | awk -F":" '{print $1}'`
    [ "${startLine}" = "0" ] && startLine=1
    if [ "`sed -n "${startLine},$"p ${LOG_NAME} | grep 'Exception'`" != "" ]; then
        echo "1"
    else
        echo "0"
    fi
}

##  HDFS command

del_data()
{
    DEL_DIR=${1}
    ${HADOOP_HOME}/bin/hadoop fs -rmr ${DEL_DIR}
}
copy_data()
{
	S_DIR=${1}
	D_DIR=${2}
	${HADOOP_HOME}/bin/hadoop fs -cp ${S_DIR} ${D_DIR}
}
move_data()
{
	S_DIR=${1}
	D_DIR=${2}
	${HADOOP_HOME}/bin/hadoop fs -mv ${S_DIR} ${D_DIR}
}

rebuild_hdfs()
{
    HOSTS_CNT="${1:-1}"

    ##  stop hadoop and mpi
    isHDFS="jps | grep NameNode"
    [ -n "$isHDFS" ] && "$_TESTDIR/stop-svr.sh" >> "$LOG_NAME" 2>&1 || onerror "stop Hadoop and MPI error"

    ##  check hosts count request
    SLAVES_CNT=`wc -l "$_TESTDIR/conf/slaves.ls" | awk '{print $1}'`
    if [ "$SLAVES_CNT" -lt "$HOSTS_CNT" ]
    then
        HOSTS_CNT=$SLAVES_CNT
    fi
    echo $HOSTS_CNT

    ##  remove the hdfs data
    echo "HADOOP_DATA_DIR: $HADOOP_DATA_DIR"
    [ -n "$HADOOP_DATA_DIR" -a "$HADOOP_DATA_DIR" != "*" ] && rm -rf $HADOOP_DATA_DIR 
    for host in `cat $_TESTDIR/conf/slaves`
    do
        [ -n "$HADOOP_DATA_DIR" -a "$HADOOP_DATA_DIR" != "*" ] && ssh $host "rm -rf $HADOOP_DATA_DIR"
    done

    ##  copy slaves configuration
    SLAVES_PATH="$_TESTDIR/conf/slaves"

    rm $SLAVES_PATH
    for host in `cat "$_TESTDIR/conf/slaves.ls"`
    do
        echo $host >> $SLAVES_PATH
        HOSTS_CNT=$((HOSTS_CNT-1))
        [ "$HOSTS_CNT" = "0" ] && break
    done
    
    cp $SLAVES_PATH $HADOOP_HOME/conf/slaves
    for DIST_HOST in `cat $_TESTDIR/conf/slaves`
    do
        echo "Copy slaves to $DIST_HOST." | tee -a "$LOG_NAME"
        scp -q "$SLAVES_PATH" "$DIST_HOST":"$HADOOP_HOME/conf/slaves" || onerror "cannot ssh to $DIST_HOST"    
    done

    ##  format HDFS    
    $HADOOP_HOME/bin/hadoop namenode -format | tee -a "$LOG_NAME" 2>&1 || onerror "format namenode error"
    sleep 2

    ##  start Hadoop
    "$HADOOP_HOME/bin/start-all.sh" || onerror "hadoop start error"

    ##  wait for Hadoop start successfully
    sleep 180
}

##  validation functions
check_tera_sort_result()
{
    startLine=`wc -l ${LOG_NAME} | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}

    cmd="${HADOOP_HOME}/bin/hadoop jar ${HADOOP_HOME}/hadoop-examples-1.2.1.jar teravalidate ${SOURCE_PATH} ${TARGET_PATH}" 
    echo $cmd | tee -a ${LOG_NAME} && (${cmd}) 2>&1 | tee -a ${LOG_NAME}

    if [ "$?" != "0" -o "`${HADOOP_HOME}/bin/hadoop fs -cat ${TARGET_PATH}/part*`" != "" ];
    then
        teraRes=`${HADOOP_HOME}/bin/hadoop fs -cat ${TARGET_PATH}/* `
         echo "[FAIL] TeraSort `basename $TARGET_PATH` result is incorrect. " | tee -a $REPORT_NAME
        TEST_STAT=1
    else
        echo "[OK] TeraSort `basename $TARGET_PATH`" | tee -a $REPORT_NAME
                del_data ${TARGET_PATH}
    fi

    startLine=0
}

check_bytes_sort_result()
{
    startLine=`wc -l ${LOG_NAME} | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}

    cmd="${HADOOP_HOME}/bin/hadoop jar ${HADOOP_HOME}/hadoop-test-1.2.1.jar testmapredsort \
        -sortInput ${SOURCE_PATH} -sortOutput ${TARGET_PATH}"
    echo $cmd | tee -a ${LOG_NAME} && (${cmd}) 2>&1 | tee -a ${LOG_NAME}

    if [ "$?" != "0" -o "`check_log ${startLine}`" != "0" ]
    then
         echo "[FAIL] Sort `basename $TARGET_PATH` result is incorrect. " | tee -a $REPORT_NAME
        TEST_STAT=1
    else
        echo "[OK] Sort `basename $TARGET_PATH`" | tee -a $REPORT_NAME
    fi

    startLine=0
}

check_wc_result()
{
    HAD_DIR=${1}
    MPID_DIR=${2}
    
    pushd $_TESTDIR_TMP
    rm ${HAD_DIR} ${MPID_DIR} -rf
    ${HADOOP_HOME}/bin/hadoop fs -get ${HAD_DIR} . 
    ${HADOOP_HOME}/bin/hadoop fs -get ${MPID_DIR} .

    isDiff=`diff ${HAD_DIR}/part* ${MPID_DIR}/part*`
    echo $isDiff | tee -a ${LOG_NAME}

    if [ "$isDiff" != "" ]
    then
        echo "[FAIL] WordCount `basename $MPID_DIR`: result is different with `basename $HAD_DIR`" | tee -a $REPORT_NAME
        TEST_STAT=1
    else
        echo "[OK] WordCount MPI-D (`basename $MPID_DIR`) has the same result with HADOOP (`basename $HAD_DIR`)" | tee -a $REPORT_NAME
    fi
    popd
}

clear_cache(){
    echo "clear cache"
    for host in localhost `cat ${MPI_D_SLAVES}`;do
        ssh -t $host "sudo sync;sudo bash -c 'echo 3 > /proc/sys/vm/drop_caches'" &> /dev/null
    done
}

sync(){
    for host in localhost `cat ${MPI_D_SLAVES}`;do
        ssh -t $host "sudo sync"
    done
}

start_dstat(){
    dname=$1
    DSTAT_HOSTNAME_ATTANCH=`head -n1 $_TESTDIR/conf/slaves`
    DSTATA_LOG="$_TESTDIR/logs/${NB_DATE}"
    mkdir -p ${DSTATA_LOG}
    for host in `cat ${MPI_D_SLAVES}`;do
        echo ssh ${host} "mkdir -p ${DSTATA_LOG}"
        ssh ${host} "mkdir -p ${DSTATA_LOG}"
    done

    logCnt=0
    tmpLogFile="${DSTATA_LOG}/dstat_${dname}.csv"
    dstat_logFile=$tmpLogFile.${logCnt}
    while [ -e "$dstat_logFile" ];do
        logCnt=$((logCnt+1))
        dstat_logFile=$tmpLogFile.${logCnt}
    done
    echo $dstat_logFile | tee -a ${LOG_NAME}

    dstat --time --cpu --mem --disk --net --output ${dstat_logFile} > /dev/null &
    dpid=$!
    dpid2=()
    cnt=0
    for host in `cat ${MPI_D_SLAVES}`;do
        ssh -t ${host} "dstat --time --cpu --mem --disk --net --output ${dstat_logFile}_${host} > /dev/null" &
        dpid2[cnt]=$!
        cnt=$((cnt+1))
    done

    sleep 30
    DSTAT_START_TAG=1
}

stop_dstat(){
    if [ "$DSTAT_START_TAG" = "1" ]; then
        DSTAT_START_TAG=0
    else
        return
    fi
    sleep 60
    kill -9 $dpid ${dpid2[*]}

    for host in `cat ${MPI_D_SLAVES}`;do
        for i in `ssh ${host} ps aux | grep dstat | awk '{print $2}'`;do
         ssh -t ${host} kill -9 $i;
        done
    done
}

change_param(){
    MAX_MEM_USED_PERCENT=$1
    PART_BUFF_SIZE=$2
    SEND_QUEUE_LENGTH=$3
    MAX_DATA_IN_MEM_PERCENT=$4

    MAX_MEM_USED_PERCENT_KEY="max.mem.used.percent"
    PART_BUFF_SIZE_KEY="partition.buffer.size"
    SEND_QUEUE_LENGTH_KEY="send.queue.length"
    MAX_DATA_IN_MEM_PERCENT_KEY="max.data.in.mem.percent"

    MAPRED_FILE="${MPI_D_HOME}/conf/mpi-d.properties"

    lineNum=`sed -e "s/^#.*/#/" $MAPRED_FILE | grep -n $MAX_MEM_USED_PERCENT_KEY | awk -F":" '{print $1}'`
    sed -i "${lineNum}s/[0-9]\+\.[0-9]\+/$MAX_MEM_USED_PERCENT/" $MAPRED_FILE
    echo "sed -i \"${lineNum}s/[0-9]\+\.[0-9]\+/$MAX_MEM_USED_PERCENT/\" $MAPRED_FILE "

    lineNum=`sed -e "s/^#.*/#/" $MAPRED_FILE | grep -n $PART_BUFF_SIZE_KEY | awk -F":" '{print $1}'`
    sed -i "${lineNum}s/[0-9]\+/$PART_BUFF_SIZE/" $MAPRED_FILE

    lineNum=`sed -e "s/^#.*/#/" $MAPRED_FILE | grep -n $SEND_QUEUE_LENGTH_KEY | awk -F":" '{print $1}'`
    sed -i "${lineNum}s/[0-9]\+/$SEND_QUEUE_LENGTH/" $MAPRED_FILE

    lineNum=`sed -e "s/^#.*/#/" $MAPRED_FILE | grep -n $MAX_DATA_IN_MEM_PERCENT_KEY | awk -F":" '{print $1}'`
    sed -i "${lineNum}s/[0-9]\+\.[0-9]\+/$MAX_DATA_IN_MEM_PERCENT/" $MAPRED_FILE

    for host in `cat ${MPI_D_SLAVES}`;do
            echo "copy mapred to ${host}"
            scp -q ${MAPRED_FILE} ${host}:${MAPRED_FILE} 2>&1 >/dev/null
    done
}
