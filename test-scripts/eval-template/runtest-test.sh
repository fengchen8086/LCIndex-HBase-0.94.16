#! /usr/bin/env bash
# Run profiling tests for distributed computing.
# Results will be recorded in "results/".
# You can specify the workload by modifing the workload.sh.
# Author : Lu CHAO(me@chao.lu)
# Date : 2014/04/22  
# Notes : Modified by Fan Liang at 2014/07/15

# Read Jobs
JOB_LIST_FILE="_job_list"
[ ! -e "$JOB_LIST_FILE" ] && echo "No job list files." && exit -1
JOB_LIST=()
cnt=0
for job in `cat ${JOB_LIST_FILE}`;do
  JOB_LIST[cnt]=$job
  cnt=$((cnt+1))
done

#STAGES=( "PERF" "DSTAT" )
STAGES=( "DSTAT" )
#STAGES=( "ALL" )

# Read execution nodes
NODES=()
cnt=0
for n in `cat conf/slaves`;do
  NODES[cnt]=$n
  cnt=$((cnt+1))
done

USER=nbtest
GROUP=nbtest
DSTAT_SLEEP=0

# Some basic functions for job execution
source basic.sh

# Initialize the test directory in each node.
TESTDIR=$(cd `dirname $0`;pwd)
for HOST in ${NODES[@]}
do
  ssh $USER@$HOST "mkdir -p $TESTDIR/tmp"
done;
mkdir -p $TESTDIR/results
rm -rf $TESTDIR/results/*

# Start the Test
for job in ${JOB_LIST[@]}
do
  # Clear the tmp logs.
  for HOST in ${NODES[@]}
  do
    ssh $USER@$HOST "rm -f $TESTDIR/tmp/*"
  done;
  JOB_LOG_DIR=$TESTDIR/results/${job}/
  mkdir -p $JOB_LOG_DIR
  
  # Name the log files
  setpath $TESTDIR $JOB_LOG_DIR
  ldfunc

  # Prepare the Job execution evironment
  clear_cache

  # Wait for idle
  sleep $DSTAT_SLEEP
  # Iterate each stage.
  for STAGE in ${STAGES[@]}
  do
    mkdir ${JOB_LOG_DIR}/${STAGE}
    echo `date +%Y-%d-%m\ %H:%M:%S` ":Testing ${job}, STAGE ${STAGE}"
    for HOST in ${NODES[@]}
    do
      case $STAGE in
      "PERF")   
      # Start perf
      #ssh $USER@$HOST "nohup sudo perf record -F 100 -e cycles,instructions,branch-misses,cache-misses,stalled-cycles-frontend -a -o $TESTDIR/tmp/PERF_${job}_${HOST}.data" &> /dev/null &;;
      ssh $USER@$HOST "nohup sudo perf record -F 100 -e cycles,instructions,branch-misses,cache-misses,stalled-cycles-frontend,L1-dcache-load-misses,L1-icache-load-misses,LLC-load-misses,dTLB-load-misses,iTLB-load-misses -a -o $TESTDIR/tmp/PERF_${job}_${HOST}.data" &> /dev/null &
       ;;
      "DSTAT")
      # Start dstat
      ssh $USER@$HOST "nohup dstat --time --cpu --mem --disk --net --output $TESTDIR/tmp/DSTAT_${job}_${HOST}.csv > /dev/null" &> /dev/null &
      ;;
      "ALL")
      # Start dstat and perf
      ssh $USER@$HOST "nohup dstat --time --cpu --mem --disk --net --output $TESTDIR/tmp/DSTAT_${job}_${HOST}.csv > /dev/null" &> /dev/null &
      ssh $USER@$HOST "nohup sudo perf record -F 100 -e cycles,instructions,branch-misses,cache-misses,stalled-cycles-frontend,L1-dcache-load-misses,L1-icache-load-misses,LLC-load-misses,dTLB-load-misses,iTLB-load-misses,rAA24,r0149,r0185 -a -o $TESTDIR/tmp/PERF_${job}_${HOST}.data" &> /dev/null &
      #ssh $USER@$HOST "nohup taskset -c 7 dstat --time --cpu --mem --disk --net -C 0,1,2,3,4,5,6 --output $TESTDIR/tmp/DSTAT_${job}_${HOST}.csv > /dev/null" &> /dev/null &
      #ssh $USER@$HOST "nohup sudo perf record -F 100 -e cycles,instructions,branch-misses,cache-misses,stalled-cycles-frontend -a -o $TESTDIR/tmp/PERF_${job}_${HOST}.data" &> /dev/null &
      #ssh $USER@$HOST "nohup sudo taskset -c 7 perf record -C 0,1,2,3,4,5,6 -F 100 -e cycles,instructions,branch-misses,cache-misses,stalled-cycles-frontend,L1-dcache-load-misses,L1-icache-load-misses,LLC-load-misses,dTLB-load-misses,iTLB-load-misses,rAA24,r0149,r0185 -a -o $TESTDIR/tmp/PERF_${job}_${HOST}.data" &> /dev/null &
      ;;
      esac
    done;

    # Start time
    t1=`date +%s`
  
    #Run job here.
    bash $TESTDIR/workload.sh ${job}
  
    clear_cache
    # End time
    t2=`date +%s`

    for HOST in ${NODES[@]}
    do
      case $STAGE in
      "PERF")   
      # End perf
      ssh $USER@$HOST "sudo killall perf -s INT";;
      "DSTAT")
      # End dstat
      ssh $USER@$HOST "sudo killall python -s INT";;
      "ALL")
      # End dstat and perf
      ssh $USER@$HOST "sudo killall perf -s INT"
      ssh $USER@$HOST "sudo killall python -s INT";;
      esac
    done;
    # Print time
    echo "With ${STAGE}: Execution Time: $((t2-t1)) sec." >> ${JOB_LOG_DIR}/time.log
    echo `date +%Y-%d-%m\ %H:%M:%S` ":End ${job},Stage ${STAGE}, Execution Time: $((t2-t1)) sec."
    # Collect the logs to local disk.
    for HOST in ${NODES[@]}
    do
      ssh $USER@$HOST "sudo chown ${USER}:${GROUP} $TESTDIR/tmp/*"
      scp -r -q $USER@$HOST:$TESTDIR/tmp/* ${JOB_LOG_DIR}/${STAGE}
    done;
  done;

  # Finalize the Job execution evironment
  sleep $DSTAT_SLEEP

  # Data process
  case $STAGE in 
  "PERF")
  for file in `ls ${JOB_LOG_DIR}/PERF/*`
  do
    echo "${file}" >> ${JOB_LOG_DIR}/perf.log
    perf report --stdio -i $file 2>/dev/null | grep -B 1 'Event count' >> ${JOB_LOG_DIR}/perf.log
  done
  ;;
  "DSTAT")
  $TESTDIR/extDstat.py ${JOB_LOG_DIR}/DSTAT ${JOB_LOG_DIR}/dstat.log
  ;;
  "ALL")
  mkdir ${JOB_LOG_DIR}/ALL/PERF -p
  mv ${JOB_LOG_DIR}/ALL/PERF_* ${JOB_LOG_DIR}/ALL/PERF
  mkdir ${JOB_LOG_DIR}/ALL/DSTAT -p
  mv ${JOB_LOG_DIR}/ALL/DSTAT_* ${JOB_LOG_DIR}/ALL/DSTAT
  for file in `ls ${JOB_LOG_DIR}/ALL/PERF/*`
  do
    echo "${file}" >> ${JOB_LOG_DIR}/perf.log
    perf report --stdio -i $file 2>/dev/null | grep -B 1 'Event count' >> ${JOB_LOG_DIR}/perf.log
  done
  $TESTDIR/extDstat.py ${JOB_LOG_DIR}/ALL/DSTAT ${JOB_LOG_DIR}/dstat.log
  ;;
  esac

done;

