#!/bin/sh

hbase_path="/home/cfeng/softwares/hbase-0.94.16"
regionserverpath=$hbase_path"/conf/regionservers"

for i in `cat $regionserverpath`; do
    echo $i;
    scp ../hbase-files/*.jar $i:$hbase_path;
done

echo "publish finish"
