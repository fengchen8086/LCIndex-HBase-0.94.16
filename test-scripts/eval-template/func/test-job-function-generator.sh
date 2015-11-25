#!/usr/bin/env bash

file="$_TESTDIR/func/test-job-function-basic.sh"
if [ -f "$file" ]; then
    . "$file"
else
    echo "No $file found."
    exit 1
fi

[ "$IMPORT_BASIC" != "true" ] && echo "Please import the basic job function script 'test-job-function-basic.sh'" && exit 1

##  source data generation
_create_date(){

    # need Set TARGET_PATH, MAPS_PER_HOSTS, MEGABYTES_PER_MAP, JOB_NAME before
    echo "Doing ${JOB_NAME} to HDFS ${TARGET_PATH}" | tee -a "$LOG_NAME"

    ${HADOOP_HOME}/bin/hadoop fs -rmr ${TARGET_PATH}
    i=3
    for((;i!=0;i--)){
        startLine=`wc -l ${LOG_NAME} | awk '{print $1}'`
    
        echo $cmd | tee -a ${LOG_NAME} && (${cmd}) 2>&1 | tee -a ${LOG_NAME}

        had_stat=`check_log $startLine`
        if [ "$had_stat" = "0" ] 
        then 
            break 
        fi
        ${HADOOP_HOME}/bin/hadoop fs -rmr ${TARGET_PATH}
        sleep 15
    }
    if [ "$had_stat" = "0" ]
    then
        echo "[OK] Hadoop ${JOB_NAME} `basename $TARGET_PATH`" | tee -a $REPORT_NAME
        ${HADOOP_HOME}/bin/hadoop fs -rmr ${TARGET_PATH}/_*
    else
        echo "[FAIL] Hadoop ${JOB_NAME} `basename $TARGET_PATH`" | tee -a $REPORT_NAME
        ${HADOOP_HOME}/bin/hadoop fs -rmr ${TARGET_PATH}
    fi

    startLine=0
}

create_text_data()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    TARGET_PATH=${1}
    MAPS_PER_HOSTS=${2}
    MEGABYTES_PER_MAP=${3}
    JOB_NAME="Text Generator"

    cmd="${HADOOP_HOME}/bin/hadoop jar \
        ${HADOOP_HOME}/hadoop-examples-1.2.1.jar randomtextwriter \
        -D test.randomtextwrite.total_bytes=$((${MAPS_PER_HOSTS} * ${MEGABYTES_PER_MAP} * 1024 * 1024 * ${HOSTS_NUM})) \
        -D test.randomtextwrite.bytes_per_map=$((${MEGABYTES_PER_MAP} * 1024 * 1024)) \
        -D test.randomtextwrite.min_words_key=1 \
        -D test.randomtextwrite.max_words_key=10 \
        -D test.randomtextwrite.min_words_value=0 \
        -D test.randomtextwrite.max_words_value=200 \
        -D mapred.output.compress=false \
        -outFormat org.apache.hadoop.mapred.TextOutputFormat \
        ${TARGET_PATH}"

    _create_date
}

create_text_data2()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    TARGET_PATH=${1}
    MAPS_PER_HOSTS=${2}
    MEGABYTES_PER_MAP=${3}
    JOB_NAME="Text Generator"

    cmd="${HADOOP_HOME}/bin/hadoop jar \
        ${HADOOP_HOME}/hadoop-examples-1.2.1.jar randomtextwriter \
        -D test.randomtextwrite.total_bytes=$((${MAPS_PER_HOSTS} * ${MEGABYTES_PER_MAP} * 1024 * 1024 * ${HOSTS_NUM})) \
        -D test.randomtextwrite.bytes_per_map=$((${MEGABYTES_PER_MAP} * 1024 * 1024)) \
        -D test.randomtextwrite.min_words_key=1 \
        -D test.randomtextwrite.max_words_key=1 \
        -D test.randomtextwrite.min_words_value=0 \
        -D test.randomtextwrite.max_words_value=20 \
        -D mapred.output.compress=false \
        ${TARGET_PATH}"

    _create_date
}

create_bytes_data()
{
    TARGET_PATH=${1}
    MAPS_PER_HOSTS=${2}
    MEGABYTES_PER_MAP=${3}
    JOB_NAME="Bytes/Sequence Generator"
    
    cmd="${HADOOP_HOME}/bin/hadoop jar \
        ${HADOOP_HOME}/hadoop-examples-1.2.1.jar randomwriter \
        -D test.randomwriter.maps_per_host=${MAPS_PER_HOSTS} \
        -D test.randomwrite.bytes_per_map=$((${MEGABYTES_PER_MAP} * 1024 * 1024)) \
        -D mapred.output.compress=false \
        ${TARGET_PATH}"
    
    _create_date
}

create_compressed_bytes_data()
{
    TARGET_PATH=${1}
    MAPS_PER_HOSTS=${2}
    MEGABYTES_PER_MAP=${3}
    JOB_NAME="Bytes/Sequence Generator"
    
    cmd="${HADOOP_HOME}/bin/hadoop jar \
        ${HADOOP_HOME}/hadoop-examples-1.2.1.jar randomwriter \
        -D test.randomwriter.maps_per_host=${MAPS_PER_HOSTS} \
        -D test.randomwrite.bytes_per_map=$((${MEGABYTES_PER_MAP} * 1024 * 1024)) \
        -D mapred.output.compress=true \
        ${TARGET_PATH}"
    
    _create_date
}

create_tera_data()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    TARGET_PATH=${1}
    MAPS_PER_HOSTS=${2}
    MEGABYTES_PER_MAP=${3}
    JOB_NAME="TeraData Generator"
    LINES_1M=10000
    LINES=$((${LINES_1M} * ${MEGABYTES_PER_MAP} * ${MAPS_PER_HOSTS} * ${HOSTS_NUM}))
 
    echo "Create ${TARGET_PATH} Tera Data" | tee -a "$LOG_NAME"

    cmd="${HADOOP_HOME}/bin/hadoop jar \
        ${HADOOP_HOME}/hadoop-examples-1.2.1.jar teragen \
        -D mapred.map.tasks=$((${MAPS_PER_HOSTS} * ${HOSTS_NUM})) \
        ${LINES} ${TARGET_PATH}"

    _create_date
}
