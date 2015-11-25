#!/usr/bin/env bash

export LOADED_CONFIG=true

# mahout
MAHOUT_HOME="/home/nbtest/develop/mahout-distribution-0.8"
MAHOUT_EXAMPLE_JAR="/home/nbtest/user/chaolu/hadoop-bench/hadoop-example.jar"

# hadoop
HADOOP_HOME="/home/nbtest/develop/hadoop-1.2.1"
HADOOP_CONF_DIR="${HADOOP_HOME}/conf"
HDFS_CONF_CORE="${HADOOP_HOME}/conf/core-site.xml" # needed by datampi
HADOOP_EXAMPLE_JAR="${HADOOP_HOME}/example.jar"
HADOOP_BENCH_JAR="$_TESTDIR/lib/hadoop-example.jar"

# datampi
MPI_D_HOME="/home/nbtest/develop/datampi-debug"
MPI_D_SLAVES="conf/slaves"
RESTART_PATH="/mnt/nbtest_data/datampi/checkpoint"
#DATAMPI_EXAMPLE_JAR="${MPI_D_HOME}/share/datampi/examples/datampi-example.jar"
#DATAMPI_EXAMPLE_JAR="${MPI_D_HOME}/share/datampi/examples/common/common-example.jar"
DATAMPI_EXAMPLE_JAR="$MPI_D_HOME/lib/datampi.jar"
DATAMPI_BENCH_JAR="$_TESTDIR/lib/datampi.jar"

# spark
SPARK_HOME="/home/nbtest/develop/spark-0.9.0-incubating-bin-hadoop1"
SPARK_MASTER="spark://172.22.1.21:7077"
HDFS_MASTER="hdfs://172.22.1.21:8000"
MAX_CORES=16 
EXEC_MEM=12g
