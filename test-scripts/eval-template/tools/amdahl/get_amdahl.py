#!/usr/bin/env python

import sys, re, csv, time, os
from os import path

class PERFLOG:
    def __init__(self):
        self.arr = {}
        self.keys = []

    def setKeyWords(self, *words):
        for w in words:
            self.keys.append(w)
            self.arr[w] = 0
        
    def parse(self, file):
        key = None
        isDo = False
        for line in open(file, 'r'):
            for w in self.keys:
                r = re.compile("'" + w + "'")
                if r.search(line):
                    key = w
                    isDo = True
                    break
            if(isDo):
                m = re.match(r".*Event count .* (\d+)", line)
                if m:
                    self.arr[key] += int(m.group(1))
                    isDo = False
                        
    def println(self):
        print self.arr
        
class DSTATLOG:
    def __init__(self):
        self.arr = []
        
    # extract the basic information from dstat csv file, the csv file format is like this:
    # Index:   [0           ,1          ,2          ,3          ,4          ,5          ,6,
    #           7           ,8          ,9          ,10         ,11         ,12         ,13          ,14]
    # Content: [time        ,cpu_usr    ,cpu_sys    ,cpu_idl    ,cpu_wai    ,cpu_hiq    ,cpu_siq,
    #           mem_used    ,mem_buff   ,mem_cach   ,mem_free   ,dsk_read   ,dsk_writ   ,net_recv    ,net_send]
    def _parser(self, csvfp, time_start=None):
        TIME_STR_FORMAT = "%d-%m %X"
        rc = re.compile("^\d+-\d+\ \d+:\d+:\d+")
        startTime = 0
        if time_start is not None:
            startTime = time.mktime(time.strptime(time_start, TIME_STR_FORMAT))
    
        tarr = []
        for row in csv.reader(open(csvfp, 'r'), delimiter=","):
            if len(row) > 0 and rc.match(row[0]):
                if startTime == 0:
                    startTime = time.mktime(time.strptime(row[0], TIME_STR_FORMAT))
                currTime = time.mktime(time.strptime(row[0], TIME_STR_FORMAT))
                if currTime < startTime:
                    continue
                tarr.append([currTime - startTime,  # time_cnt 1
                    100 - (float(row[3]) + float(row[9]) + float(row[15]) + float(row[21]) + float(row[27]) + float(row[33]) + float(row[39])) / 7,  # cpu_used 2
                    (float(row[4]) + float(row[10]) + float(row[16]) + float(row[22]) + float(row[28]) + float(row[34]) + float(row[40])) / 7,  # cpu_iowait 3
                    float(row[43]),  # mem_used    MB 4
                    float(row[44]),  # mem_buff    MB 5
                    float(row[45]),  # mem_cach    MB 6
                    float(row[46]),  # mem_free    MB 7
                    float(row[47]),  # dsk_read    MB/s 8
                    float(row[48]),  # dsk_writ    MB/s 9
                    float(row[49]),  # net_recv    Kb/s 10
                    float(row[50])])  # net_send    Kb/s 11
        return tarr    

    # simply fill the empty array
    def _fill(self, arr):
        if len(arr) == 0:
            return
        pre_data = arr[0]
        new_arr = [pre_data]
        for cur_data in arr[1:]:
            if pre_data[0] + 1 < cur_data[0]:
                span = cur_data[0] - pre_data[0]
                for i in range(1, int(span)):
                    new_data = list(pre_data)
                    new_data[0] += i
                    new_arr.append(new_data)
                new_arr.append(cur_data)
                pre_data = list(cur_data)
        return new_arr
            
    def parse_dir(self, dpath, time_start=None, time_span=1):
        self.arr = []
        for f in os.listdir(dpath):
            self.arr.append(self._fill(self._parser(path.join(dpath, f), time_start)))
        return self.arr
        
def get_amdahl_number(dstat_arr, perf_arr, dstat_bias, execute_time):
    total_mem_per_node = 16710455296
    instructions = perf_arr['instructions']
    t1, t2 = dstat_bias, dstat_bias + execute_time
    dlen = len(dstat_arr)
    dstat_arr = [[sum(var) for var in zip(*tarr)] for tarr in zip(*dstat_arr)]
    dstat_arr = map(lambda x:[x[0] / dlen] + x[1:] , dstat_arr)
    dstat_range = [item for item in dstat_arr if item[0] > t1 and item[0] < t2]
    sum_number = [sum(item) for item in zip(*dstat_range)]
    min_number = [min(item) for item in zip(*dstat_range)]
    ips = instructions / execute_time

    return {'exec_time':execute_time,
            'cpu_cnt':8 * dlen,
            'gips':float(ips) / (1000 ** 3),
            'ram':(total_mem_per_node * dlen - min_number[6]) / (1024 ** 3),
            'netio':(sum_number[9] + sum_number[10]) / execute_time / (1024.0 ** 2),
            'diskio':(sum_number[8] + sum_number[7]) / execute_time / (1024.0 ** 2),
            'amdahl_ram':(total_mem_per_node * dlen - min_number[6]) / float(ips),
            'amdahl_disk':(sum_number[8] + sum_number[7]) * 8 / 1000 / float(ips),
            'amdahl_io':(sum_number[8] + sum_number[7] + sum_number[9] + sum_number[10]) * 8 / 1000 / float(ips)}
    
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Usage: %s [exec time file] [dstat dir] [perf log]" % sys.argv[0]
        sys.exit(-1)
    
    m = re.match(r".*cost (\d+).*", open(sys.argv[1]).readline())
    
    exec_time = 0
    if m:
        exec_time = float(m.group(1))

    dstat = DSTATLOG()
    dstat.parse_dir(sys.argv[2])
    perf = PERFLOG()
    perf.setKeyWords("cycles", "instructions", "stalled-cycles-frontend", "branch-misses", "cache-misses",
                     "L1-dcache-load-misses", "L1-icache-load-misses", "LLC-load-misses",
                     "dTLB-load-misses", "iTLB-load-misses", "rAA24", "r0149", "r0185")
    perf.parse(sys.argv[3])
    
    result = get_amdahl_number(dstat.arr, perf.arr, 0, exec_time)
    print """
               CORE Count: %d
               Time (sec): %d
                     GIPS: %.2f
                 RAM (GB): %.2f
            NET IO (MB/s): %.2f
           DISK IO (MB/s): %.2f
Amdahl RAM (byte per ins): %.5f
Amdahl DISK (bit per ins): %.5f
  Amdahl IO (bit per ins): %.5f    
    """ % (result['cpu_cnt'], result['exec_time'], result['gips'], result['ram'], result['netio'],
           result['diskio'], result['amdahl_ram'], result['amdahl_disk'], result['amdahl_io'])

    for key in perf.keys:
        print "%+25s: %d" % (key, perf.arr[key])
