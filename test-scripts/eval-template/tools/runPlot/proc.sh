#!/usr/bin/env bash

for file in `ls *data.bak`; do
    #mv $file{,.bak}
    awk '{
        if(NR%2==0){
            print $0
        }
    }' $file > ${file%.bak}
done
