#!/usr/bin/env python
# coding=UTF-8

# AHEAD - Advanced Hadoop Exact Algorithm for Distances
# Programa desenvolvido para o projeto de pesquisa entitulado "Um Algoritmo Paralelo Eficiente para Cálculo da Centralidade em Grafos."

# Referências:

# CRUZ, L. C. ; MURTA, C. D. . Um Algoritmo Paralelo Eficiente para Cálculo da Centralidade em Grafos.
# In: XIV Simpósio em Sistemas Computacionais (WSCAD-SSC), 2013, Porto de Galinhas PE. 
# Anais do XIV Simpósio em Sistemas Computacionais (WSCAD-SSC). Porto Alegre: Sociedade Brasileira de Computação, 2013. p. 3-10.

# Leonardo Carlos da Cruz. Um Algoritmo Paralelo Eficiente para Cálculo da Centralidade em Grafos. 2013. 
# Dissertação (Mestrado em Modelagem Matemática e Computacional)
# Centro Federal de Educação Tecnológica de Minas Gerais, . Orientador: Cristina Duarte Murta.

# Programa desenvolvido em conjunto com Gessy Caetano Júnior - Laboratório de Computação Científica da UFMG.

import datetime
import nltk
import re
import time
import urllib2
from dateutil import parser
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection


class jobinfo:


    def __init__(self,patternfile):
        self.patternfile = patternfile
        self.info = []

    def _findInfo(self, pattern, data):
        match = re.findall(pattern, data)
        info = []
        if match:
            if ( 'Map' in pattern ) or ( 'Reduce' in pattern ) and \
                not ( 'Reduce shuffle bytes' in pattern ):
                for index in range(0, len(match[0]) - 2):
                    info.append(int(match[0][index].replace(",", "")))
                timeinms = timeconvert()
                info.append(timeinms[match[0][4]])
                info.append(timeinms[match[0][5]])
            else:
                for index in range(0, len(match[0])):
                    info.append(int(match[0][index].replace(",", "")))
        return info

    def __getitem__(self, joburl):
        jobpage = urllib2.urlopen(joburl)
        jobsourcepage = jobpage.read()
        rawpage = nltk.clean_html(jobsourcepage)

        def timetominutes(timelist):
            if len(timelist) == 1:
                return int(timelist[0]) / 60.0
            elif len(timelist) == 2:
                return int(timelist[0]) + int(timelist[1]) / 60.0
            elif len(timelist) == 3:
                return int(timelist[0]) * 60 + int(timelist[1]) + \
                        int(timelist[2]) / 60.0
            elif len(timelist) == 4:
                return int(timelist[0]) * (24 * 60) + int(timelist[1]) * 60 + \
                        int(timelist[2]) + int(timelist[3]) / 60.0
            else:
                return 0
            
        try:
            patterns = open(self.patternfile, 'r')
        except IOError:
            print "Pattern file not found, exiting gracefully"
        else:
            if re.search(r'Status: SUCCESS',rawpage):
                jobId = re.search(r'job[_\d+]*\d+',joburl)
                self.info = {'jobId': jobId.group() }
                self.info.update({'jobURL': joburl })
                for line in patterns:
                    metricname = line.split(";")[0]
                    pattern = (line.split(";")[1])[:-1]
                    metric = self._findInfo(pattern, rawpage)
                    self.info.update({metricname: metric})
        finally:
            patterns.close()
            jobstart = re.findall(r'Launched At: (\d+-\w+-\d+ \d+:\d+:\d+)',
                                    rawpage)
            timeinms = timeconvert()
            jobstart = timeinms[jobstart[0]]
            self.info.update({'jobstart': jobstart})
        return self.info


class taskinfo:


    def __init__(self, joburl):
        self.joburl = joburl

    def __getitem__(self, key):

        if (key[0] in ( 'MAP', 'REDUCE') and
            key[1] in ( 'all', 'SUCCESS', 'FAILED', 'KILLED' )):
            taskurl = self.joburl.replace('jobdetailshistory.jsp?',
                'jobtaskshistory.jsp?') + "&taskType=" + key[0] + "&status=" +\
                     key[1]
            jobpage = urllib2.urlopen(taskurl)
            jobsourcepage = jobpage.read()
            rawpage = nltk.clean_html(jobsourcepage)
            taskpattern = r'(task_\d+_\d+_\w_\d+) (\d+/\d+ \d+:\d+:\d+)' +\
                            r' (\d+/\d+ \d+:\d+:\d+)'
            taskslist = re.findall(taskpattern, rawpage)
            tasksinfo=[]
            timeinms = timeconvert()
            for tasktuple in taskslist:
                tasksinfo.append((tasktuple[0],
                                    timeinms[tasktuple[1]],
                                    timeinms[tasktuple[2]]))                                    
            return tasksinfo
    
    def tasksgraph(self, taskdict, epsfile, graphlabels):
        alltasks = taskdict.values()[0] + taskdict.values()[1]
        ymax = len(alltasks) + 10
        fig = plt.figure()
        ax = fig.add_subplot(111)
        y = 1
        
        for tasktype in taskdict.keys():
            segments = []
            
            for tasktuple in sorted(taskdict[tasktype], key=lambda tup: tup[1]):
                segments.append(((tasktuple[1],y),(tasktuple[2],y)))
                y = y + 1

            if tasktype == 'Map':
                linecolor = '0.2'
            else:
                linecolor = '0.8'
                            
            line_segments = LineCollection(segments, linewidths=2,
                color=linecolor, label=tasktype)
            ax.add_collection(line_segments)


        allvalues = map( sorted, zip(*alltasks))[1] + \
                    map( sorted, zip(*alltasks))[2]
        
        xmin = min(allvalues)
        xmax = max(allvalues)                    
        
        ax.set_title(graphlabels[0])
        ax.set_ylabel(graphlabels[1])
        ax.set_xlabel(graphlabels[2])
        ax.legend(graphlabels[3], loc=2, frameon=False)
        ax.set_ylim(0,ymax)
        ax.set_xlim(xmin ,xmax)
        locs,labels = plt.xticks()
        plt.xticks(locs, map(lambda x: "%g" % x, locs-min(locs)), fontsize=7)
        plt.text(0.82, -0.06, "+%g" % min(locs), fontsize=9, 
                    transform = plt.gca().transAxes)
        ax.grid(True)
        plt.savefig(epsfile, format='eps', dpi=600 )


class  joblist:


    def __init__(self, chosendatesfile, jobhistorypage):
        self.chosendatesfile = chosendatesfile
        self._jobhistorypage = jobhistorypage
        self.chosenjoburls = []

    def _chosenDates(self):
        try:
            datetimesfile = open(self.chosendatesfile, 'r')
        except IOError:
            print "Input date times not found, exiting gracefully"
        else:
            chosendates = []
            timeinms = timeconvert()
            for date in datetimesfile:
                chosendates.append((timeinms[date.split(",")[0]],
                        timeinms[(date.split(",")[1])[:-1]]) )
        finally:
            datetimesfile.close()
        return chosendates

    def _catchURLJobs(self):
        joburllist = []
        pagejobs = urllib2.urlopen(self._jobhistorypage)
        sourcepage =  pagejobs.read()
        urlpattern=r'"(jobdetailshistory\.jsp\?logFile=file:' +\
                    '[/\w+-.]+/\w+_\d+_/[\d+/]+job[_\d+]+_hadoop_.*)"'
        
        joburllist = re.findall(urlpattern, sourcepage)        
        for jobindex in range(0,len(joburllist)):
                joburllist[jobindex] = 'http://localhost:50030/' + \
                                joburllist[jobindex]
        return joburllist

    def _chooseURLJobs(self, joblist, datetuple):
        chosenjobs = []
        for job in joblist:
            if  ( int(job.split("_")[5]) >= datetuple[0]  and  
                    int(job.split("_")[5]) <= datetuple[1] ):
                chosenjobs.append(job)
        chosenjobs.reverse()
        return chosenjobs

    def chosenjobset(self):
        datelist = self._chosenDates()
        for datetuple in datelist:
            joburllist = self._catchURLJobs()
            self.chosenjoburls.append(self._chooseURLJobs(joburllist,datetuple))
        return self.chosenjoburls



class timeconvert:


    def __getitem__(self,datetime):
        parseropt = parser.parserinfo(dayfirst=True)
        datestruct = parser.parse(datetime, parserinfo=parseropt)
        dateinms = int( datestruct.strftime( "%s" ) + '000' )
        return dateinms
