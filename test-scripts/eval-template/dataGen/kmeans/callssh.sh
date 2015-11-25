#/bin/sh

echo "before run kmeans on lingcloud${1}"
nohup ssh lingcloud${1} /mnt/sda/kmeans.py 80 1 &
#nohup ssh lingcloud${1} /mnt/sda/tosleep.sh &

echo "after run kmeans on lingcloud${1}"
