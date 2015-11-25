#!/bin/sh


fun_CheckStorage(){
    checkStorageOut=`pwd`/storage.dat
    > $checkStorageOut
    for i in `cat ~/hbase-nodes`; do 
        echo $i
        echo $i >> $checkStorageOut
        ssh hec-$i "du -s /home/fengchen/data" | tee -a $checkStorageOut
    done
    mv $checkStorageOut $1
}

fun_CheckStorage

