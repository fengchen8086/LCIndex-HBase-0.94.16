#!/usr/bin/env python
"""
This script can touch down a results_dir into a xlsx file.
Author              : Lu Chao (me@chao.lu)
Created Date    : 2014/07/21, 14:29
Depend on       : xlsxwriter
"""

import sys, re, csv, time, os, string
import xlsxwriter
from os import path

class PERFLOG:
    def __init__(self):
        self.arr = {} 
        self.newarr = {}

    def parse(self, file):
        key = None
        isDo = False
        for line in open(file, 'r'):
            r = re.compile("(?<=')[^']+(?=')")
            res = r.findall(line)
            if len(res):
                key = res[0]
                if key not in self.arr:
                    self.arr[key] = 0
                isDo = True
                continue
            if(isDo):
                m = re.match(r".*Event count .* (\d+)", line)
                if m:
                    self.arr[key] += int(m.group(1))
                    isDo = False

    def generalize(self):
        instructions = self.arr["instructions"]
        for key in self.arr:
            self.newarr[key] = float(self.arr[key]) / instructions

    def println(self):
        print self.arr
class ExcelGenerator:
    def __init__(self):
        self.jobs = {}
        self.platforms = []
        self.types = {"dm":"DataMPI", "had":"Hadoop", "spk":"Spark"}
    def getJob(self, result_path): 
        rePm = re.compile("(?<=results-)\w+")
        reJob = re.compile("[^_]+")
        for dirPlatform in os.listdir(result_path): 
            if os.path.isdir(os.path.join(result_path, dirPlatform)):
                curPlatform = rePm.findall(dirPlatform)[0]
                if curPlatform not in self.platforms:
                    self.platforms.append(curPlatform)
                for dirJob in os.listdir(os.path.join(result_path, dirPlatform)):
                    if os.path.isdir(os.path.join(result_path, dirPlatform,dirJob)):
                        curJob = reJob.findall(dirJob)[1]
                        curSize = reJob.findall(dirJob)[0]
                        if curJob not in self.jobs:
                            self.jobs[curJob] = {}
                        if curPlatform not in self.jobs[curJob]:
                            self.jobs[curJob][curPlatform] = {}
                        self.jobs[curJob][curPlatform][curSize] = os.path.join(result_path, dirPlatform, dirJob)
        self.platforms.sort()

    def makeChart(self, job, sheet, pos, label_pos, output_row):
        transcode = string.maketrans("0123456789","ABCDEFGHIJ")
        series_label = "="
        tmp = []
        sorted_label_pos = label_pos.keys()
        sorted_label_pos.sort()
        for label in sorted_label_pos:
            tmp.append(job+"!$A$"+str(label_pos[label]+1))
        series_label += ",".join(tmp)
        for key in pos: 
            chart = workbook.add_chart({'type': 'column'})
            chart.set_title({'name': key})
            for platform in pos[key]:
                series = "="
                tmp = []
                for item in pos[key][platform]:
                    tmp.append(job+"!$"+str(item[1]).translate(transcode)+"$"+str(item[0]+1))
                series += ",".join(tmp)
                print series
                chart.add_series({'values': series,'categories':series_label,'name':self.types[platform]})
            output_row += 10
            sheet.insert_chart('A'+str(output_row), chart)

    def genExcel(self):
        for job in self.jobs:
            col = 1
            row = 0
            worksheet = workbook.add_worksheet(job)
            worksheet.write(row,col,"Events")
            pos = {}
            label_pos = {}
            for platform in  self.platforms:
                col += 1
                row = 0
                if platform in self.types:
                    worksheet.write(row,col,self.types[platform])
                    worksheet.write(row,col+len(self.platforms),self.types[platform])
                else:
                    print platform+" is not a defined platform"
                    None
                row = 1
                sort_keys = self.jobs[job][platform].keys()
                sort_keys.sort()
                for data_size in sort_keys:
                    worksheet.write(row,0,data_size)
                    if data_size not in label_pos:
                        label_pos[data_size] = row
                    perf = PERFLOG()
                    perf.parse(os.path.join(self.jobs[job][platform][data_size], "perf.log")) 
                    perf.generalize();
                    for key in perf.arr:
                        worksheet.write(row,1,key)
                        worksheet.write(row,col,perf.arr[key])
                        worksheet.write(row,col+len(self.platforms),perf.newarr[key])
                        if key not in pos :
                            pos[key] = {}
                        if platform not in pos[key]:
                            pos[key][platform] = []
                        pos[key][platform].append((row, col+len(self.platforms)))
                        row += 1
                    row +=1
            self.makeChart(job, worksheet, pos, label_pos , row)


    def printJob(self):
        print self.jobs


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s [results dir]" % sys.argv[0]
        sys.exit(-1)
    
    exgen = ExcelGenerator()
    exgen.getJob(sys.argv[1])

    workbook = xlsxwriter.Workbook('perfs.xlsx')
    exgen.genExcel()
    workbook.close()
