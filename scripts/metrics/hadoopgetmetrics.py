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


import argparse
import os
from hadoopjobmetrics import *


JOBHISTORYPAGE='http://localhost:50030/jobhistoryhome.jsp' +\
                '?pageno=-1&search=&scansize=2'


def writeheader(filename, opertype):
    try:
        fw = open(filename, opertype)
    except IOError:
        print "Could not write to file: " + filename
    else:
        fw.writelines(  'jobId\t' +
                        'launched Date\t' +
                        'slots_millis_maps\t' + 
                        'slots_millis_reduces\t' + 
                        'cpu_time_spent_(ms)\t\t\t' +
                        'physical_memory_snapshot\t\t\t' + 
                        'virtual_memory_snapshot\t\t\t' +
                        'reduce_shuffle_bytes\t' +
                        'file_bytes_read\t\t\t' + 
                        'hdfs_bytes_read\t\t\t' +
                        'file_bytes_writen\t\t\t' +
                        'hdfs_bytes_written\t\t\t' +
                        'map tasks\t\t\t\t' +
                        'map start date\t' + 
                        'map finished date\t' +
                        'reduce tasks\t\t\t\t' + 
                        'reduce start date\t' +
                        'reduce finished date\t' +
                        'AtualizacoesDeDistancias\n'
                        )
                    
        fw.writelines(  '\t\t\t\t' +
                        'map\treduce\ttotal\t' +
                        'map\treduce\ttotal\t' +
                        'map\treduce\ttotal\t\t' + 
                        'map\treduce\ttotal\t' +
                        'map\treduce\ttotal\t' +
                        'map\treduce\ttotal\t' +
                        'map\treduce\ttotal\t' + 
                        'total\tsuccess\tfailed\tkiled\t\t\t' + 
                        'total\tsuccess\tfailed\tkiled\t\n'
                        )    
    finally:
        fw.close()



def writeinfo(filename, job):
    try:
        fw = open(filename, 'a')
    except IOError:
        print "Could not write to file: " + filename
    else:
        jobmetrics = [  job['jobId'],
                        job['jobstart'],
                        job['slots_millis_maps'][2],
                        job['slots_millis_reduces'][2],
                        job['cpu_time_spent_(ms)'][0],
                        job['cpu_time_spent_(ms)'][1],
                        job['cpu_time_spent_(ms)'][2],
                        job['physical_memory_snapshot_(bytes)'][0],
                        job['physical_memory_snapshot_(bytes)'][1],
                        job['physical_memory_snapshot_(bytes)'][2],
                        job['virtual_memory_snapshot_(bytes)'][0],
                        job['virtual_memory_snapshot_(bytes)'][1],
                        job['virtual_memory_snapshot_(bytes)'][2],
                        job['reduce_shuffle_bytes'][2],
                        job['file_bytes_read'][0],
                        job['file_bytes_read'][1],
                        job['file_bytes_read'][2],
                        job['hdfs_bytes_read'][0],
                        job['hdfs_bytes_read'][1],
                        job['hdfs_bytes_read'][2],
                        job['file_bytes_written'][0],
                        job['file_bytes_written'][1],
                        job['file_bytes_written'][2],
                        job['hdfs_bytes_written'][0],
                        job['hdfs_bytes_written'][1],
                        job['hdfs_bytes_written'][2],
                        job['map_tasks'][0],
                        job['map_tasks'][1],
                        job['map_tasks'][2],
                        job['map_tasks'][3],
                        job['map_tasks'][4],
                        job['map_tasks'][5],
                        job['reduce_tasks'][0],
                        job['reduce_tasks'][1],
                        job['reduce_tasks'][2],
                        job['reduce_tasks'][3],
                        job['reduce_tasks'][4],
                        job['reduce_tasks'][5] ]

        if  job['atualizacoes_de_distancias']:
            jobmetrics.append( job['atualizacoes_de_distancias'][2] )
           
        for metric in jobmetrics:
            fw.writelines(str(metric) + "\t")

        fw.writelines("\n")
        
    finally:
        fw.close()

def writefile(filename,data,mode):
    try:
        fw = open(filename, mode)
    except IOError:
        print "Could not write to file: " + filename
    else:
        fw.writelines(data)
    finally:
        fw.close()



def taskgraph(joblist, destdir, labels):

    joblist.sort(key=lambda tup: tup[2])

    for run in list(set(zip(*joblist)[-1])):    
        indexfile = destdir + '/run' + str(run) + '/graph_index.txt'
        writefile(indexfile,'Graph_Name\t\t\t' + ' '*23 +
                'Jobtime (ordered)\n', 'w')
    
    for job in joblist:
        task = taskinfo(job[1])
        taskmap = task[('MAP', 'all')]
        taskreduce = task[('REDUCE', 'all')]
        testdict = { 'Map' : taskmap, 'Reduce' : taskreduce }
        outputgraph = destdir + '/run' + str(job[3]) + '/' + job[0] + '.eps'
        task.tasksgraph(testdict, outputgraph, labels)
        indexfile = destdir + '/run' + str(job[3]) + '/graph_index.txt'
        writefile(indexfile, job[0] + '.eps\t\t\t' + str(job[2]) + '\n', 'a')        


def main():

    parser = argparse.ArgumentParser(prog='HadoopGetMetrics')
    parser = argparse.ArgumentParser(description='Generate hadoop job metrics')
    parser.add_argument('datesfile', help='File with date intervals')
    parser.add_argument('patterns', help='File with data patterns')
    parser.add_argument('outputdir', help='Directory for write outputs')

    args = vars(parser.parse_args())

    datefile = args['datesfile']
    patterns = args['patterns']
    outputdir = args['outputdir']
    
    currentdir = os.getcwd()
    if outputdir not in os.listdir(currentdir):
    
        os.mkdir(outputdir)
        jobs = joblist(datefile,JOBHISTORYPAGE)
        jobset = jobs.chosenjobset()
        jobsinfo = jobinfo(patterns)
        jobtasks = []
        
        for index in range(0,len(jobset)):
            run = index + 1
            os.mkdir(outputdir + '/run' + str(run)) 
            outputfile = outputdir + '/run' + \
                        str(run) + '/hadoop_job_metrics.txt' 
            writeheader(outputfile,'w')

            for jobs in jobset[index]:
                job = jobsinfo[jobs]
                writeinfo(outputfile, job)
                jobtasks.append((job['jobId'], job['jobURL'],
                    job['reduce_tasks'][-1] - job['jobstart'], run))

        graphlabels = [ u'Distribuição das Tarefas no cluster',
                        'Tarefas',
                        'Data em Epoch time',
                        ( 'Tarefas Map', 'Tarefas Reduce') ]


        taskgraph(jobtasks,outputdir, graphlabels)

    else:
        raise Exception, 'Output Directory Already Exists'

main()
