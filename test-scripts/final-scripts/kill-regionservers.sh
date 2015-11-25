#!/bin/sh

for i in `cat conf/regionservers`; do
    echo $i;
#    pid=`ssh $i jps | grep HQuorumPeer | cut -d ' ' -f 1`
#    ssh $i kill -9 $pid
    pid=`ssh $i jps | grep HRegionServer | cut -d ' ' -f 1`
    ssh $i kill -9 $pid
done

pid=`jps | grep HMaster | cut -d ' ' -f 1`
kill -9 $pid
#pid=`jps | grep HQuorumPeer | cut -d ' ' -f 1`
#kill -9 $pid

echo "kill regionservers done"

