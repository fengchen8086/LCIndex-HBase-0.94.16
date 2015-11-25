#!/bin/sh

for i in `cat ./zks`; do
    echo $i;
    ssh $i zkServer.sh stop
    ssh $i "mv /home/fengchen/data/zookeeper/myid /home/fengchen/data/myid"
    ssh $i "rm /home/fengchen/data/zookeeper/* -rf"
    ssh $i "mv /home/fengchen/data/myid /home/fengchen/data/zookeeper/myid"
done

echo "clear zk done"


