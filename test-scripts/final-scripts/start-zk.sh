#!/bin/sh

for i in `cat ./zks`; do
    echo $i;
    ssh $i zkServer.sh start
done

echo "zk started"


