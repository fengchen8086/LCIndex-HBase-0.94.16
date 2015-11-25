#!/usr/bin/env bash

file="$_TESTDIR/func/test-job-function-basic.sh"
if [ -f "$file" ]; then
    . "$file"
else
    echo "No $file found."
    exit 1
fi

[ "$IMPORT_BASIC" != "true" ] && echo "Please import the basic job function script 'test-job-function-basic.sh'" && exit 1

_test_mpi_start()
{
    MPI_NOACK_TAG='no msg recvd from mpd when expecting ack of request'

    startLine=$1
    startLine=$((startLine+1))
    isNoAck=`sed -n "${startLine},$"p ${LOG_NAME} | grep "${MPI_NOACK_TAG}"`
    if [ "$isNoAck" != "" ]
    then
        echo 1
    else
        echo 0
    fi
}

_check_mpi_start()
{
    startLine=$1
    PId=$2
    chk_cnt=3
    while [ "$chk_cnt" != "0" ]
    do
        if [ "`_test_mpi_start $startLine`" = "1" ]
        then
            kill  $PId
            for mpid in `jps | grep MPI_D | awk '{print $1}'`;do kill $mpid;done
            kill $PId
            echo 1
            break
        else
            sleep 2
        fi
        chk_cnt=$((chk_cnt - 1))
    done
    echo 0
}

##  DataMPI jobs
_do_mpid_func()
{
    # need set TARGET_PATH, SOURCE_PATH, JOB_NAME, HADOOP_HOME, LOG_NAME, REPORT_NAME
#   del_data ${TARGET_PATH}
    isSrcExist=`${HADOOP_HOME}/bin/hadoop fs -ls ${SOURCE_PATH}`
    if [ "$isSrcExist" = "" ]
    then
        echo "[FAIL] DataMPI $JOB_NAME `basename $SOURCE_PATH` MAPS $MAPS REDUCES $REDS. Doesn't have $SOURCE_PATH" | tee -a $REPORT_NAME
        return 1
    fi

    i=1
    for((;i!=0;i--)){
        [ ! -f "${LOG_NAME}" ] && touch ${LOG_NAME}
        startLine=`wc -l "$LOG_NAME" | awk '{print $1}'`
        echo ${cmd} | tee -a ${LOG_NAME}
		t1=`date +%s`
		case ${cmd} in
			"f_pagerank")
			run_pagerank 2>&1 | tee -a ${LOG_NAME} &
			;;
			*)
        	bash -c "${cmd}" 2>&1 | tee -a ${LOG_NAME} &
			;;
		esac
        PId=$!  
        if [ "`_check_mpi_start $startLine $PId`" = "0" ] 
        then 
            break 
        fi
    }

    if [ "$i" != "0" ]
    then
        wait $PId
        
        if [ "$?" != "0" -o "`check_log ${startLine}`" != "0" ]
        then
             echo "[FAIL] DataMPI $JOB_NAME `basename $SOURCE_PATH` MAPS $MAPS REDUCES $REDS. " | tee -a $REPORT_NAME
            TEST_STAT=1
        else
		    t2=`date +%s`
            costTime=`get_running_time 'mpid' ${startLine}`
            echo "[OK] DataMPI $JOB_NAME `basename $SOURCE_PATH` hosts $HOSTS_NUM app spend ${costTime}, total cost $((t2-t1)) sec" | tee -a $REPORT_NAME
        fi
    else
        echo "[FAIL] DataMPI $JOB_NAME `basename $SOURCE_PATH` MAPS $MAPS REDUCES $REDS. mpi start with no ack recv " | tee -a $REPORT_NAME
    fi

    startLine=0
}

do_tera_st_mpid()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$((${4} * ${HOSTS_NUM}))
    JOB_NAME="terast"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_EXAMPLE_JAR} mpid.examples.TeraSortOnHDFSDataLocal \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"

    _do_mpid_func
}

do_text_wc_mpid_local()
{
    ##  use text data
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$MAPS
    JOB_NAME="textwclocal"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_EXAMPLE_JAR} mpid.examples.WordCountOnHDFSDataLocal \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"

    _do_mpid_func
}

do_text_wc_mpid()
{
    ##  use text data
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    JOB_NAME="textwc"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A 1 \
        -jar ${DATAMPI_EXAMPLE_JAR} mpid.examples.WordCountOnHDFS \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"

    _do_mpid_func
}

do_text_sort_mpid()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$((${4} * ${HOSTS_NUM}))
    JOB_NAME="textst"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_EXAMPLE_JAR} mpid.examples.SortOnHDFS \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"

    _do_mpid_func
}

do_text_sort_mpid_local()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$((${4} * ${HOSTS_NUM}))
    JOB_NAME="textstlocal"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_EXAMPLE_JAR} mpid.examples.SortOnHDFSDataLocal \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"

    _do_mpid_func
}

do_bytes_sort_mpid()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$((${4} * ${HOSTS_NUM}))
    JOB_NAME="seqst"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_EXAMPLE_JAR} mpid.examples.BytesSortOnHDFS \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH}"

    _do_mpid_func
}

do_text_grep_mpid()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$MAPS
    JOB_NAME="textgrep"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_EXAMPLE_JAR} mpid.examples.GrepOnHDFSDataLocal \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH} princess"

    _do_mpid_func

}

do_dmbdb_sort_mpid()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
    TARGET_PATH=${2}
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$((${4} * ${HOSTS_NUM}))
    JOB_NAME="seqst"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_EXAMPLE_JAR} dmb.Sort \
        ${HDFS_CONF_CORE} ${SOURCE_PATH} ${TARGET_PATH} bytes"

    _do_mpid_func
}

do_kmeans_mpid()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    POINT_PATH=${1}
    TARGET_PATH=${2}
    CENTERS_PATH=${TARGET_PATH}/center0
    MAPS=$((${3} * ${HOSTS_NUM}))
    REDS=$((${4} * ${HOSTS_NUM}))
    KCENTERS=${5}
    ITER=${6:-1}
    JOB_NAME="kmeans"

    cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
        -mode COM -O ${MAPS} -A ${REDS} \
        -jar ${DATAMPI_BENCH_JAR} \
        dmb.kmeans.KmeansInit \
        ${POINT_PATH} ${CENTERS_PATH} $KCENTERS"

    isSrcExist=`${HADOOP_HOME}/bin/hadoop fs -ls ${SOURCE_PATH}`
    if [ "$isSrcExist" = "" ]
    then
        echo "[FAIL] DataMPI $JOB_NAME `basename $SOURCE_PATH` MAPS $MAPS REDUCES $REDS. Doesn't have $SOURCE_PATH" | tee -a $REPORT_NAME
        return 1
    fi

    [ ! -f "${LOG_NAME}" ] && touch ${LOG_NAME}
    echo ${cmd} | tee -a ${LOG_NAME}
	t1=`date +%s`
    ${cmd} 2>&1 | tee -a ${LOG_NAME}
    t2=`date +%s`
    echo "Prepare cost $((t2-t1)) sec" | tee -a ${LOG_NAME}

    for i in `seq $ITER`; do

        PREV_CENTERS_PATH=$CENTERS_PATH
        CENTERS_PATH="$TARGET_PATH/out${i}"

        cmd="${MPI_D_HOME}/bin/mpidrun -f ${MPI_D_SLAVES} \
            -mode COM -O ${MAPS} -A ${REDS} \
            -jar ${DATAMPI_BENCH_JAR} \
            dmb.kmeans.KmeansIter \
            ${POINT_PATH} ${PREV_CENTERS_PATH} ${CENTERS_PATH} $KCENTERS"

        echo ${cmd} | tee -a ${LOG_NAME}
        ${cmd} 2>&1 | tee -a ${LOG_NAME}
        t3=`date +%s`
        echo "Iter $i cost $((t3-t2)) sec" | tee -a ${LOG_NAME}
        t2=$t3

    done

    echo "[OK] DataMPI $JOB_NAME `basename $POINT_PATH` total cost $((t2-t1)) sec" | tee -a $REPORT_NAME
}
# $1 is edges , $2 is vecs, $3 is outputs, $4 is iters, $5 is maps ,$6 is reds
do_pagerank_mpid()
{
    HOSTS_NUM=`wc -l $_TESTDIR/conf/slaves | awk '{print $1}'`

    SOURCE_PATH=${1}
	VEC_PATH=${2}
	PRVEC_PATH="pr_vector"
	TMP_PATH="pr_tempmv"
    TARGET_PATH=${3}
	let "I=2**${4}"
	MAX_ITERS=${5}
	MAPS=$((${6} * ${HOSTS_NUM}))
    REDS=$((${7} * ${HOSTS_NUM}))
    JOB_NAME="pagerank"
	del_data $TMP_PATH
	del_data $PRVEC_PATH
	copy_data $VEC_PATH $PRVEC_PATH
	cmd="f_pagerank"
    _do_mpid_func
    del_data $TMP_PATH
	del_data $PRVEC_PATH
}
run_pagerank()
{
    for i in `seq 1 $MAX_ITERS`;
    do
	echo "ITER: loop $i"
	echo "First Stage ......." 
    "${MPI_D_HOME}/bin/mpidrun" \
        -f ${MPI_D_SLAVES} -mode COM \
        -O "${MAPS}" -A "${REDS}"  \
        -jar $DATAMPI_BENCH_JAR dmb.web.pagerank.PagerankNaive \
        "${HDFS_CONF_CORE}" "${SOURCE_PATH}" "${PRVEC_PATH}" "${TMP_PATH}"; 
	echo "Second Stage ......"
    "${MPI_D_HOME}/bin/mpidrun" \
        -f ${MPI_D_SLAVES} -mode COM \
        -O "${MAPS}" -A "${REDS}"  \
        -jar $DATAMPI_BENCH_JAR dmb.web.pagerank.PagerankMerge \
        "${HDFS_CONF_CORE}" "${TMP_PATH}" "${TARGET_PATH}" \
        "${I}" "0.85" ; 
    del_data ${PRVEC_PATH}; 
    del_data ${TMP_PATH}; 
    if [ ! $i = $MAX_ITERS ]
    then 
        move_data ${TARGET_PATH} ${PRVEC_PATH} 
    fi 
    done
}
