#/bin/sh

#fileSize 
#fileSize=250
#targetTotalSize=20
#((targetFileNum=${targetTotalSize}*20))

#echo $targetFileNum

for i in `seq 22 28`;do scp kmeans.py lingcloud$i:`pwd`; done
for i in `seq 22 28`;do scp tosleep.sh  lingcloud$i:`pwd`; done
for i in `seq 22 28`;do scp callssh.sh  lingcloud$i:`pwd`; done

#for i in `seq 22 28`;do ssh lingcloud$i /mnt/sda/tosleep.sh &; done

#for i in `seq 21 28`;do nohup ssh lingcloud$i /mnt/sda/kmean.py 10 1; done

for i in `seq 21 28`; do ./callssh.sh $i ; done

