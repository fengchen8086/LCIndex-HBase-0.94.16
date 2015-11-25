#!/usr/bin/env bash
# Convert EPS file to PDF file. Work with `runPlot.sh`.
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

pushd $workDir > /dev/null
# Gnuplot each job.
for job in `ls $workDir`
do
    if [ -d "$job" ]; then
        pushd $job > /dev/null
        curDir=`pwd`
        for file in `ls $curDir/*.eps`; do
            epstopdf $file 
            pdfcrop ${file%.eps}.pdf
        done
        popd > /dev/null
    else
        file=$job
        case $file in 
            *.eps)
                epstopdf $file 
                pdfcrop ${file%.eps}.pdf
                ;;
        esac
    fi
done

popd > /dev/null
