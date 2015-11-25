#!/bin/sh

rm /home/fengchen/data/tmp/hbase-0.94.16 -rf
rm /home/fengchen/data/lccindex/* -rf

for i in `cat conf/regionservers`; do
    echo $i;
    ssh $i rm /home/fengchen/data/lccindex/* -rf
    ssh $i rm /home/fengchen/data/tmp/hbase-0.94.16/* -rf
done

hadoop fs -rmr /hbase

echo "clear hbase files done"

