#!/bin/sh

for i in `cat ./clients`; do
    echo $i;
    pid=`ssh $i jps | grep TPCHRemoteServer | cut -d ' ' -f 1`
    ssh $i kill -9 $pid
    pid=`ssh $i jps | grep TPCHRemoteClient | cut -d ' ' -f 1`
    ssh $i kill -9 $pid
done

echo "kill remote tests done"

