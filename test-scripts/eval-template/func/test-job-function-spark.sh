#!/bin/bash

SPARK_LAUNCH="${SPARK_HOME}/bin/spark-class"
file="$_TESTDIR/func/test-job-function-basic.sh"
if [ -f "$file" ]; then
    . "$file"
else
    echo "No $file found."
    exit 1
fi

[ "$IMPORT_BASIC" != "true" ] && echo "Please import the basic job function script 'test-job-function-basic.sh'" && exit 1

##  SPARK jobs
_do_spark_func()
{
    # need set TARGET_PATH, SOURCE_PATH, JOB_NAME, HADOOP_HOME, LOG_NAME, REPORT_NAME
#   del_data ${TARGET_PATH}
    isSrcExist=`${HADOOP_HOME}/bin/hadoop fs -ls ${SOURCE_PATH}`
    if [ "$isSrcExist" = "" ]
    then
        echo "[FAIL] Spark ${JOB_NAME} `basename $SOURCE_PATH`. Doesn't have $SOURCE_PATH" | tee -a $REPORT_NAME
        return 1
    fi

    i=1
    for((;i!=0;i--)){
        [ ! -f "${LOG_NAME}" ] && touch ${LOG_NAME}
        startLine=`wc -l ${LOG_NAME} | awk '{print $1}'`
        t1=`date +%s`
        echo $cmd | tee -a ${LOG_NAME} && (${cmd}) 2>&1 | tee -a ${LOG_NAME}
        t2=`date +%s`

        spark_stat=`check_log $startLine`
        if [ "$spark_stat" = "0" ] 
        then 
            break 
        fi
        del_data ${TARGET_PATH}
        sleep 15
    }
    if [ "$spark_stat" = "0" ]
    then
        echo "[OK] Spark ${JOB_NAME} `basename $SOURCE_PATH` cost $((t2-t1)) sec" | tee -a $REPORT_NAME
    else
        echo "[FAIL] Spark ${JOB_NAME} `basename $SOURCE_PATH`" | tee -a $REPORT_NAME
        del_data ${TARGET_PATH}
    fi
    del_data "${SOURCE_PATH}/_*"

    startLine=0
}

do_text_sort_spark()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    JOB_NAME="textst"

    cmd="${SPARK_LAUNCH} scala.Sort \
        $SPARK_MASTER \
        ${HDFS_MASTER}/${SOURCE_PATH} \
        ${HDFS_MASTER}/${TARGET_PATH} ${MAX_CORES} ${EXEC_MEM}"

    _do_spark_func
}

do_text_wc_spark()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    JOB_NAME="wordcount"

    cmd="${SPARK_LAUNCH} scala.WordCount \
        $SPARK_MASTER \
        ${HDFS_MASTER}/${SOURCE_PATH} \
        ${HDFS_MASTER}/${TARGET_PATH} ${MAX_CORES} ${EXEC_MEM}"

    _do_spark_func
}

do_text_grep_spark()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    REGEX=${3}
    GROUP=${4}
    JOB_NAME="grep"

    cmd="${SPARK_LAUNCH} scala.Grep \
        ${SPARK_MASTER} \
        ${HDFS_MASTER}/${SOURCE_PATH} \
        ${HDFS_MASTER}/${TARGET_PATH} \
        ${REGEX} \
        ${GROUP} ${MAX_CORES} ${EXEC_MEM}"

    _do_spark_func
}

do_kmeans_spark()
{
    SOURCE_PATH=${1}
	KCluster=${2}
    ITER=${3}
    JOB_NAME="kmeans"

    cmd="${SPARK_HOME}/bin/run-example org.apache.spark.mllib.clustering.KMeans \
        ${SPARK_MASTER} \
        ${HDFS_MASTER}/${SOURCE_PATH} \
    	${KCluster} \
    	${ITER} \
        ${MAX_CORES} ${EXEC_MEM} 1"
#   cmd="${SPARK_HOME}/bin/run-example org.apache.spark.examples.SparkKMeans \
#       ${SPARK_MASTER} \
#       ${HDFS_MASTER}/${SOURCE_PATH} \
#   	${KCluster} \
#   	${ITER} \
#       ${MAX_CORES} ${EXEC_MEM}"

    _do_spark_func
}

