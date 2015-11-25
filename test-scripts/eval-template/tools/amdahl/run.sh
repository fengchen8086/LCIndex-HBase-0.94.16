#!/usr/bin/env bash
curDir=`pwd`
for dir in `ls ${curDir}`;do 
    [ -d "$dir" ] && echo $dir && python get_amdahl1.py ${dir}/app_report_* ${dir}/ALL/DSTAT/ ${dir}/perf.log
done
