#!/usr/bin/env python
import os, random, threading, sys
import socket

dataDir="/mnt/sda"
file_size = 250*1024*1024
dimension = 20
fot = -200
rng = 200
hostname = socket.gethostname()

def _create(file_prefix, file_number, file_size):
    os.chdir(dataDir)
    for k in range(file_number):
        outFile = file_prefix + "." + k.__str__()
        if os.path.exists(outFile):
            cur_size = os.path.getsize(outFile)
            if (abs(cur_size - file_size) * 1.0 / file_size) < 0.01:
                print outFile + " exists and similar size, continue."
                continue
            else:
                print outFile + " exists but size not match, target(" + str(file_size) + ") meet cur(" + str(cur_size) + ")"
        out = open(outFile, "w")
        while os.stat(outFile).st_size < file_size:
            lineStr = ""
            for j in range(dimension):
                lineStr = lineStr + " %.4f" % (random.random() * rng + fot)
            lineStr = lineStr[1:] + "\n"
            out.write(lineStr)
        print out.name + " complete."

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: %s [file number] [thread number]" % sys.argv[0]
        sys.exit(-1)
    file_number = int(sys.argv[1])
    thread_number = int(sys.argv[2])
    
    min_file_per_thread = file_number / thread_number
    thread_arr = [min_file_per_thread] * thread_number
    residue_file = file_number - thread_number * min_file_per_thread
    
    cnt = 0
    for i in range(thread_number):
        if cnt < residue_file:
            thread_arr[i] += 1
        cnt += 1

    outFilePre = "kmeans.data.%s" % hostname
        
    ts = []
    for i in range(thread_number):
        if thread_arr[i] > 0:
            t = threading.Thread(target=_create, args=(outFilePre + str(i), thread_arr[i], file_size))
            ts.append(t)
    
    for t in ts:
        t.start()
    
    for t in ts:
        t.join()
