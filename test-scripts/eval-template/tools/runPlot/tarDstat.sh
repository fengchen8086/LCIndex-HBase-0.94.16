#! /usr/bin/env bash
# Collect platforms' dstat.log and tar into a figures.tgz file.
# Run: ./tarDstat.sh [Results_DIR]
# Author : Lu CHAO(me@chao.lu)
# Date : 2014/04/26
#
# The directory structure should be like this
# |result
# |   ├── results-dm
# │   │   ├── 20G_SORT_DM
# │   │   │   ├── dstat.log
# 

curDir=`pwd`
if [ "$#" -ne "1" ]; then
    echo "Usage: ./runPlot.sh [Results_DIR]"
    exit 1
fi

# Collect the data
if [ -d "$1" ];then
  mkdir -p "figures/"
  for dir_p in `ls $1/`
  do
    platform=`echo $dir_p|awk -F '-' '{print $2}'`
    for dir_j in ` ls -F $1/$dir_p |grep "/$"`
    do
       job=`echo $dir_j|awk -F '_' '{print $1 "_" $2}'`
       if [ ! -d "$curDir/figures/$job" ];then
          mkdir -p "$curDir/figures/$job"
       fi
       cp $1/$dir_p/$dir_j/dstat.log $curDir/figures/$job/$platform.data
    done
  done
fi
tar cvzf figures.tgz figures/
