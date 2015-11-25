#!/bin/sh

run_classpath=".:a.jar:hbase-0.94.16.jar"
cd lib
for i in `ls *.jar`; do
    run_classpath=${run_classpath}:`pwd`/$i;
done

cd ../

java -cp $run_classpath aid.CheckLCFiles /home/fengchen/softwares/hadoop-1.0.4 /home/fengchen/data/lccindex

