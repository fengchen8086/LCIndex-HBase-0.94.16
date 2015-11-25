#!/bin/sh

hbase_path=`pwd`
rm $hbase_path/logs/* -rf;
for i in `cat conf/regionservers`; do
    echo $i;
    ssh $i rm $hbase_path/logs/* -rf;
done

echo "clear logs done"


