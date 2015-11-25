#!/usr/bin/env python
# coding: utf-8

import csv, sys, re, time, os
from os import path


# extract the basic information from dstat csv file, the csv file format is like this:
# Index:   [0           ,1           ,2           ,3           ,4           ,5           ,6,
#                       ,7           ,8           ,9           ,10          ,11          ,12,
#                       ,13          ,14          ,15          ,16          ,17          ,18,
#                       ,19          ,20          ,21          ,22          ,23          ,24,
#                       ,25          ,26          ,27          ,28          ,29          ,30,
#                       ,31          ,32          ,33          ,34          ,35          ,36,
#                       ,37          ,38          ,39          ,40          ,41          ,42,
#           43          ,44          ,45         ,46         ,47         ,48         ,49         ,50]
# Content: [time        ,cpu_usr    ,cpu_sys    ,cpu_idl    ,cpu_wai    ,cpu_hiq    ,cpu_siq,
#           mem_used    ,mem_buff   ,mem_cach   ,mem_free   ,dsk_read   ,dsk_writ   ,net_recv    ,net_send]
# return the array of several points and the format of each point is like this:
# Index:   [1       , 2       , 3       , 4       , 5        , 6       , 7]
# Content: [time_cnt, cpu_used, mem_used, dsk_read, dsk_write, net_recv, net_send]
def parser(csvfp, time_start=None):
    TIME_STR_FORMAT = "%d-%m %X"
    rc = re.compile("^\d+-\d+\ \d+:\d+:\d+")
    startTime = 0
    if time_start is not None:
        startTime = time.mktime(time.strptime(time_start, TIME_STR_FORMAT))

    tArr = []
    for row in csv.reader(open(csvfp, 'r'), delimiter=","):
        if len(row) > 0 and rc.match(row[0]):
            if startTime == 0:
                startTime = time.mktime(time.strptime(row[0], TIME_STR_FORMAT))
            currTime = time.mktime(time.strptime(row[0], TIME_STR_FORMAT))
            if currTime < startTime:
                continue
            tArr.append([currTime - startTime,  # time_cnt 1
                    100 - (float(row[3]) + float(row[9]) + float(row[15]) + float(row[21]) + float(row[27]) + float(row[33]) + float(row[39])) / 7,  # cpu_used 2
                    (float(row[4]) + float(row[10]) + float(row[16]) + float(row[22]) + float(row[28]) + float(row[34]) + float(row[40])) / 7,  # cpu_iowait 3
                    float(row[43]) / 1024.0 / 1024,  # mem_used    MB 4
                    float(row[44]) / 1024.0 / 1024,  # mem_buff    MB 5
                    float(row[45]) / 1024.0 / 1024,  # mem_cach    MB 6
                    float(row[46]) / 1024.0 / 1024,  # mem_free    MB 7
                    float(row[47]) / 1024.0 / 1024,  # dsk_read    MB/s 8
                    float(row[48]) / 1024.0 / 1024,  # dsk_writ    MB/s 9
                    float(row[49]) / 1024.0,  # net_recv    Kb/s 10
                    float(row[50]) / 1024.0])  # net_send    Kb/s 11
    return tArr

def opd(outf, dpath, time_start=None, time_span=1):
    tarrs = []
    for f in os.listdir(dpath):
        tarrs.append(parser(path.join(dpath, f), time_start))
    __write(outf, [[sum(var) / len(var) for var in zip(*tarr)] for tarr in zip(*tarrs)], time_span)

def opf(outf, csvfp, time_start=None, time_span=1):
    __write(outf, parser(csvfp, time_start), time_span)

def __write(outf, data, time_span):
    tarr = []
    tend = time_span
    out = open(outf, 'w')
    for d in data:
        if d[0] == 0:
            out.write(" ".join(map((lambda i: ("%.2f" % i)), d)) + "\n")
        if d[0] <= tend:
            tarr.append(d)
        else:
            nr = [sum(var) / len(var) for var in zip(*tarr)]
            if len(nr) != 0:
#                 out.write("%d %.2f %.2f %.2f %.2f %.2f %.2f\n" % (tarr[-1][0],nr[1],nr[2],nr[3],nr[4],nr[5],nr[6]))
                nr[0] = tend
                out.write(" ".join(map((lambda i: ("%.2f" % i)), nr)) + "\n")
                tarr = [d]
            tend += time_span
    if len(tarr) != 0:
        nr = [sum(var) / len(var) for var in zip(*tarr)]
        nr[0] = tend
        out.write(" ".join(map((lambda i: ("%.2f" % i)), nr)) + "\n")
    out.close()

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print 'Usage: %s [input file path] ([output filename])' % sys.argv[0]
        sys.exit(-1)

    fp = sys.argv[1]
    ofp = len(sys.argv) == 3 and  sys.argv[2] or "dstat.dat"
    if path.isdir(fp):
        # when input path is directory
        opd(ofp, fp, None, 5)
    else:
        opf(ofp, fp, None, 10)
