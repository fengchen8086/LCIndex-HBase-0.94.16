#! /usr/bin/env bash
# Workload
# $1 specifies the job type. 
# Author : Lu CHAO(me@chao.lu)
# Date : 2014/04/22  
# Modified by : Fan Liang

source basic.sh
ldfunc

case $1 in
  "test_mpid")
  S_DIR=tiny
  M_TAR=tiny-mpid
  do_text_wc_mpid_local "${S_DIR}" "${M_TAR}-wc" "1"
#  do_text_sort_mpid_local "${S_DIR}" "${M_TAR}-st" "1" "1"
#  do_text_grep_mpid "${S_DIR}" "${M_TAR}-gp" "1" 
  ;;
  "test_had")
  S_DIR=tiny
  H_TAR=tiny-had
  do_text_sort_had "${S_DIR}" "${H_TAR}-st" "1"
#  do_text_wc_had "${S_DIR}" "${H_TAR}-wc"
#  do_text_grep_had "${S_DIR}" "${H_TAR}-gp" 
  ;;
  "test_spark")
MAX_CORES=4
EXEC_MEM=12g
  S_DIR=/user/nbtest/tiny
  P_TAR=/user/nbtest/tiny-spark
  do_text_sort_spark $S_DIR ${P_TAR}
#  do_text_wc_spark ${S_DIR} ${P_TAR}
#  do_text_grep_spark $S_DIR $P_TAR "princess" "0"
  ;;
# Hadoop Jobs
  "20G_SORT_HAD")
  S_DIR=/data/text/bench/20G-text
  H_TAR=20G-had-st
  do_text_sort_had "${S_DIR}" "${H_TAR}" "4"
  ;;
  "10G_WC_HAD")
  S_DIR=/data/text/bench/10G-text
  H_TAR=10G-had-wc
  do_text_wc_had "${S_DIR}" "${H_TAR}"
  ;;
  "20G_WC_HAD")
  S_DIR=/data/text/bench/20G-text
  H_TAR=20G-had-wc
  do_text_wc_had "${S_DIR}" "${H_TAR}"
  ;;
  "20G_GREP_HAD")
  S_DIR=/data/text/bench/20G-text
  H_TAR=20G-had-gp
  do_text_grep_had "${S_DIR}" "${H_TAR}" 
  ;;

  "40G_SORT_HAD")
  S_DIR=/data/text/bench/40G-text
  H_TAR=40G-had-st
  do_text_sort_had "${S_DIR}" "${H_TAR}" "4"
  ;;
  "40G_WC_HAD")
  S_DIR=/data/text/bench/40G-text
  H_TAR=40G-had-wc
  do_text_wc_had "${S_DIR}" "${H_TAR}"
  ;;
  "40G_GREP_HAD")
  S_DIR=/data/text/bench/40G-text
  H_TAR=40G-had-gp
  do_text_grep_had "${S_DIR}" "${H_TAR}" 
  ;;

  "80G_SORT_HAD")
  S_DIR=/data/text/bench/80G-text
  H_TAR=80G-had-st
  do_text_sort_had "${S_DIR}" "${H_TAR}" "4"
  ;;
  "80G_WC_HAD")
  S_DIR=/data/text/bench/80G-text
  H_TAR=80G-had-wc
  do_text_wc_had "${S_DIR}" "${H_TAR}"
  ;;
  "80G_GREP_HAD")
  S_DIR=/data/text/bench/80G-text
  H_TAR=80G-had-gp
  do_text_grep_had "${S_DIR}" "${H_TAR}" 
  ;;

  "20G_TERA_HAD")
  S_DIR=/data/text/bench/20G-tera
  H_TAR=20G-had-tera
  do_tera_st_had "${S_DIR}" "${H_TAR}" "4"
  ;;
  "40G_TERA_HAD")
  S_DIR=/data/text/bench/40G-tera
  H_TAR=40G-had-tera
  do_tera_st_had "${S_DIR}" "${H_TAR}" "4"
  ;;
  "80G_TERA_HAD")
  S_DIR=/data/text/bench/80G-tera
  H_TAR=80G-had-tera
  do_tera_st_had "${S_DIR}" "${H_TAR}" "4"
  ;;
  "20_PR5_HAD")
  S_DIR=20_edge_pagerank
  V_DIR=20_vec_pagerank
  H_TAR=20_pagerank_had

  do_pagerank_had $S_DIR $V_DIR $H_TAR 20 5 4
  ;;
  "40G_KM5_HAD")
  S_DIR=40G-km
  H_TAR=40G-km-had
  CENTERS_PATH="/user/nbtest/km-seed/part-randomSeed"

  do_kmeans_had $S_DIR $H_TAR $CENTERS_PATH 4 40 5 0.5
  ;;


# DataMPI Jobs
  "20G_SORT_DM")
  S_DIR=/data/text/bench/20G-text
  M_TAR=20G-mpid-st
  do_text_sort_mpid_local "${S_DIR}" "${M_TAR}" "4" "4"
  ;;
  "20G_WC_DM")
  S_DIR=/data/text/bench/20G-text
  M_TAR=20G-mpid-wc
  do_text_wc_mpid_local "${S_DIR}" "${M_TAR}" "4"
  ;;
  "20G_GREP_DM")
  S_DIR=/data/text/bench/20G-text
  M_TAR=20G-mpid-gp
  do_text_grep_mpid "${S_DIR}" "${M_TAR}" "4" 
  ;;

  "40G_SORT_DM")
  S_DIR=/data/text/bench/40G-text
  M_TAR=40G-mpid-st
  do_text_sort_mpid_local "${S_DIR}" "${M_TAR}" "4" "4"
  ;;
  "40G_WC_DM")
  S_DIR=/data/text/bench/40G-text
  M_TAR=40G-mpid-wc
  do_text_wc_mpid_local "${S_DIR}" "${M_TAR}" "4"
  ;;
  "40G_GREP_DM")
  S_DIR=/data/text/bench/40G-text
  M_TAR=40G-mpid-gp
  do_text_grep_mpid "${S_DIR}" "${M_TAR}" "4" 
  ;;

  "80G_SORT_DM")
  S_DIR=/data/text/bench/80G-text
  M_TAR=80G-mpid-st
  do_text_sort_mpid_local "${S_DIR}" "${M_TAR}" "4" "4"
  ;;
  "80G_WC_DM")
  S_DIR=/data/text/bench/80G-text
  M_TAR=80G-mpid-wc
  do_text_wc_mpid_local "${S_DIR}" "${M_TAR}" "4"
  ;;
  "80G_GREP_DM")
  S_DIR=/data/text/bench/80G-text
  M_TAR=80G-mpid-gp
  do_text_grep_mpid "${S_DIR}" "${M_TAR}" "4" 
  ;;

  "20G_TERA_DM")
  S_DIR=/data/text/bench/20G-tera
  M_TAR=20G-mpid-tera
  do_tera_st_mpid "${S_DIR}" "${M_TAR}" "4" "4"
  ;;
  "40G_TERA_DM")
  S_DIR=/data/text/bench/40G-tera
  M_TAR=40G-mpid-tera
  do_tera_st_mpid "${S_DIR}" "${M_TAR}" "4" "4"
  ;;
  "80G_TERA_DM")
  S_DIR=/data/text/bench/80G-tera
  M_TAR=80G-mpid-tera
  do_tera_st_mpid "${S_DIR}" "${M_TAR}" "4" "4"
  ;;
  "40G_KM5_DM")
  S_DIR=/data/text/bench/40G-km
  M_TAR=40G-km-mpid

  do_kmeans_mpid $S_DIR $M_TAR 4 4 40 5
  ;;
  "20_PR5_DM")
  S_DIR=20_edge_pagerank
  V_DIR=20_vec_pagerank
  H_TAR=20_pagerank_mpid

  do_pagerank_mpid $S_DIR $V_DIR $H_TAR 20 5 1 4
  ;;



# Spark Jobs
  "20G_SORT_SPK")
  S_DIR=/user/nbtest/20G-text
  P_TAR=/user/nbtest/20G-spark-st
  do_text_sort_spark $S_DIR ${P_TAR}
  ;;
  "20G_WC_SPK")
  S_DIR=/user/nbtest/20G-text
  P_TAR=/user/nbtest/20G-spark-wc
  do_text_wc_spark ${S_DIR} ${P_TAR}
  ;;
  "20G_GREP_SPK")
  S_DIR=/user/nbtest/20G-text
  P_TAR=/user/nbtest/20G-spark-gp
  do_text_grep_spark $S_DIR $P_TAR "princess" "0"
  ;;

# self-test
  "datampi-pagerank")
  S_DIR=/pagerank/data/pegas/edges
  V_DIR=/pagerank/data/pegas/vec
  H_TAR=/pagerank/hadoop

  do_pagerank_mpid $S_DIR $V_DIR $H_TAR 8 1 1 4
  ;;

  "hadoop-pagerank")
  S_DIR=/pagerank/data/hadoop/soc-LiveJournal1/edges
  V_DIR=/pagerank/data/hadoop/soc-LiveJournal1/vec
  H_TAR=/pagerank/hadoop

  do_pagerank_had $S_DIR $V_DIR $H_TAR 19 5 2
  ;;

  *)
  echo "Job undefined!";;
esac
