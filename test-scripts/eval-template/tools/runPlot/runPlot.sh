#! /usr/bin/env bash
# Run gnuplot for 3 platforms' dstat.log.
# Run: ./runPlot.sh [Results_DIR]
# Author : Lu CHAO(me@chao.lu)
# Date : 2014/04/26
# Modified : Fan Liang(chcdlf@gmail.com)

curDir=`pwd`
if [ "$#" -ne "1" ]; then
    echo "Usage: ./runPlot.sh [Figures_DIR]"
    exit 1
fi

figDir=$(readlink -f $1)
[ ! -d "$figDir" ] && echo "\"$figDir\" does not exist!" && exit -1

# Gnuplot each job.
for job in `ls $figDir`
do
  echo "Ploting job: $job"
  pushd $figDir/$job > /dev/null
  gnuplot $curDir/plotDstat2.dem
  popd > /dev/null
done

