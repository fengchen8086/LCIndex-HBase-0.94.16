#!/usr/bin/env bash

source conf/conf.sh

hadoop fs -rmr *-gp *-st *-wc *-mpid *-had *-spark

# execute hadoop jobs
JOB_LIST=( 
  "80G_TERA_HAD"
)

rm _job_list
for job in ${JOB_LIST[@]}; do
  echo $job >> _job_list
done

bash $HADOOP_HOME/bin/start-mapred.sh
sleep 5
bash ./runtest-test.sh
bash $HADOOP_HOME/bin/stop-mapred.sh

mv results/ results-had

# execute datampi jobs
JOB_LIST=( 
 "80G_TERA_DM"
)

rm _job_list
for job in ${JOB_LIST[@]}; do
  echo $job >> _job_list
done

bash ./runtest-test.sh

mv results/ results-dm
rm _job_list

