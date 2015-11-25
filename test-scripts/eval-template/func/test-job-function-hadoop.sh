#!/usr/bin/env bash

file="$_TESTDIR/func/test-job-function-basic.sh"
if [ -f "$file" ]; then
    . "$file"
else
    echo "No $file found."
    exit 1
fi

[ "$IMPORT_BASIC" != "true" ] && echo "Please import the basic job function script 'test-job-function-basic.sh'" && exit 1

##  Hadoop jobs
_do_hadoop_func()
{
    # need set TARGET_PATH, SOURCE_PATH, JOB_NAME, HADOOP_HOME, LOG_NAME, REPORT_NAME
    del_data ${TARGET_PATH}
    isSrcExist=`${HADOOP_HOME}/bin/hadoop fs -ls ${SOURCE_PATH}`
    if [ "$isSrcExist" = "" ]
    then
        echo "[FAIL] Hadoop ${JOB_NAME} `basename $SOURCE_PATH`. Doesn't have $SOURCE_PATH" | tee -a $REPORT_NAME
        return 1
    fi

    i=1
    for((;i!=0;i--)){
        [ ! -f "${LOG_NAME}" ] && touch ${LOG_NAME}
        startLine=`wc -l ${LOG_NAME} | awk '{print $1}'`
        t1=`date +%s`
        echo $cmd | tee -a ${LOG_NAME} && (${cmd}) 2>&1 | tee -a ${LOG_NAME}
        t2=`date +%s`

        had_stat=`check_log $startLine`
        if [ "$had_stat" = "0" ] 
        then 
            break 
        fi
        sleep 15
        #del_data ${TARGET_PATH}
    }
    if [ "$had_stat" = "0" ]
    then
        echo "[OK] Hadoop ${JOB_NAME} `basename $SOURCE_PATH` cost $((t2-t1)) sec" | tee -a $REPORT_NAME
    else
        echo "[FAIL] Hadoop ${JOB_NAME} `basename $SOURCE_PATH`" | tee -a $REPORT_NAME
        del_data ${TARGET_PATH}
    fi
    del_data "${SOURCE_PATH}/_*"

    startLine=0
}

do_tera_st_had()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    REDS=$((${3} * ${HOSTS_NUM}))
    JOB_NAME="terast"
    
    cmd="${HADOOP_HOME}/bin/hadoop jar \
        ${HADOOP_EXAMPLE_JAR} org.apache.hadoop.examples.terasort.TeraSort \
        -D mapred.reduce.tasks=${REDS} \
        ${SOURCE_PATH} ${TARGET_PATH}"

    _do_hadoop_func
}

do_text_wc_had()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    REDS=1
    JOB_NAME="textwc"
    
    cmd="${HADOOP_HOME}/bin/hadoop jar \
        ${HADOOP_EXAMPLE_JAR} org.apache.hadoop.examples.WordCount \
        ${SOURCE_PATH} ${TARGET_PATH} $REDS"

    _do_hadoop_func
}

do_text_sort_had()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    REDS=$((${3} * ${HOSTS_NUM}))
	JOB_NAME="textst"

    cmd="${HADOOP_HOME}/bin/hadoop jar \
        ${HADOOP_EXAMPLE_JAR} org.apache.hadoop.examples.Sort -r ${REDS} \
        -outKey org.apache.hadoop.io.Text \
        -outValue org.apache.hadoop.io.Text \
        -inFormat org.apache.hadoop.mapred.KeyValueTextInputFormat \
        -outFormat org.apache.hadoop.mapred.TextOutputFormat \
        ${SOURCE_PATH} ${TARGET_PATH}"

    _do_hadoop_func
}

do_seq_sort_had()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    REDS=$((${3} * ${HOSTS_NUM}))
    JOB_NAME="seqst"

    cmd="${HADOOP_HOME}/bin/hadoop jar \
        ${HADOOP_EXAMPLE_JAR} org.apache.hadoop.examples.Sort -r ${REDS} \
        ${SOURCE_PATH} ${TARGET_PATH}"

    _do_hadoop_func
}

do_text_grep_had()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
#   REDS=$((${3} * ${HOSTS_NUM}))
    JOB_NAME="textgrep"

    cmd="${HADOOP_HOME}/bin/hadoop jar \
        ${HADOOP_EXAMPLE_JAR} org.apache.hadoop.examples.Grep \
        ${SOURCE_PATH} ${TARGET_PATH} princess"

    _do_hadoop_func
}

do_kmeans_had()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    CENTERS_PATH=${3}
    REDS=$((${4} * ${HOSTS_NUM}))
    K=${5}
    ITER=${6}
    CONVERGEDIST=${7}
    JOB_NAME="kmeans"

    cmd="$MAHOUT_HOME/bin/mahout hadoop jar $MAHOUT_EXAMPLE_JAR \
        org.ict.hadoop.kmeans.KmeansMahout \
        $SOURCE_PATH $TARGET_PATH $K $ITER $CONVERGEDIST $REDS $CENTERS_PATH"

    _do_hadoop_func
}
# Input: $1 is edges path. $2 is vector path, $3 is output path. $4 is number of nodes,5 is the maxium reds per node.
do_pagerank_had()
{ 
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
	TMP_PATH="pr_tempmv"
	VEC_PATH=${2}
	PRVEC_PATH="pr_vector"
	del_data $PRVEC_PATH
    copy_data $VEC_PATH $PRVEC_PATH
    TARGET_PATH=${3}
	let "I=2**${4}"
	MAX_ITERS=${5}
    REDS=$((${6} * ${HOSTS_NUM}))
    JOB_NAME="pagerank"

	del_data $TMP_PATH

    cmd="${HADOOP_HOME}/bin/hadoop jar \
        ${HADOOP_BENCH_JAR} org.ict.pegasus.pagerank.PagerankNaive \
        ${SOURCE_PATH} ${PRVEC_PATH} ${TMP_PATH} ${TARGET_PATH} $I $REDS ${MAX_ITERS} nosym new"

    _do_hadoop_func
	del_data $TMP_PATH
	del_data $PRVEC_PATH

}

# 
do_seqfromdir_had()
{
    INPUT=$1
    OUTPUT=$2
    RED_NUM=$3
 
    echo -e "\nConvert sequence files from directory\n"
 
    cmd="$HADOOP_HOME/bin/hadoop jar \
		${HADOOP_BENCH_JAR} org.ict.mahout.bayes.train.SequenceFilesFromDirectory \
        -i $SOURCE_PATH -o $TARGET_PATH -chunk 256 -ow"
 
    echo $cmd && ${cmd} 2>&1 | tee -a ${LOG_NAME}

    echo "\n Get sequence file in ${TARGET_PATH}/seq2"
}
do_nbtrain_had()
{
	echo
}
