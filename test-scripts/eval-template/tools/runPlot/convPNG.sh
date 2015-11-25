#!/usr/bin/env bash
# Convert EPS file to PNG file. Work with `runPlot.sh`.
# Run: ./runPlot.sh [Results_DIR]
# Author : Fan Liang(chcdlf@gmail.com)
# Date : 2014/07/18

curDir=`pwd`
if [ "$#" -ne "1" ]; then
    echo "Usage: ./$0 [Figures_DIR]"
    exit 1
fi

workDir=$(readlink -f $1)
[ ! -d "$workDir" ] && echo "\"$workDir\" does not exist!" && exit -1

# Gnuplot each job.
for job in `ls $workDir`
do
  echo "Ploting job: $job"
  pushd $workDir/$job > /dev/null
  for file in `ls *.eps`; do
    convert -verbose -density 300 -trim $file -quality 200 ${file%.eps}.png
  done
  popd > /dev/null
done

