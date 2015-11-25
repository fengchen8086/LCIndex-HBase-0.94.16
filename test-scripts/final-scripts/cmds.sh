
exit

for i in `cat ~/allnodes`; do echo $i && scp -r conf hec-$i:$PWD;done

for i in `cat ~/allnodes`; do echo $i && ssh hec-$i cp ~/hbase-0.94.16.jar /home/fengchen/softwares/hbase-0.94.16/; done

for i in `cat ~/allnodes`; do echo $i && ssh hec-$i "cat /home/fengchen/softwares/hbase-0.94.16/logs/hbase-fengchen-regionserver-hec-$i.out | grep LCCHFileMoverServer"; done

cat /home/fengchen/softwares/hbase-0.94.16/logs/hbase-fengchen-regionserver-hec-*.out | grep "memstore flush"

for i in `cat ~/allnodes`; do echo $i && ssh hec-$i "ls -lh /home/fengchen/softwares/hbase-0.94.16/logs/*.out"; done

for i in `ls ./`; do echo $i && cat $i|grep "winter file not found";done

hadoop fsck /hbase/tpch_raw/ -files -locations -blocks


