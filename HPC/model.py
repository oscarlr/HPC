#!/bin/env python
import subprocess
from os import environ,system
from random import randint
import sys
from time import sleep

SLEEPTIME=60
WALLTIME=24
CPU=1
NODES=1

class Master:
    def __init__(self):
        self.alloc = environ.get('SJOB_DEFALLOC')
        if self.alloc == None:
            sys.exit('Initiate default allocation with ' 
                     'SJOB_DEFALLOC environment')
        self.job_eo = environ.get('SJOB_OUTPUT')
        if self.job_eo == None:
            self.job_eo = "%s/lsf-output" % environ['HOME']
        self.slaves = []

    def add_job(self,bash_fn,walltime=WALLTIME,cpu=CPU,
                mem=CPU*2.5,nodes=NODES):
        my_slave = Slave(bash_fn,walltime,cpu,mem,nodes,self.job_eo)
        self.slaves.append(my_slave)
    
    def jobs_done(self):
        for slave in self.slaves:
            if slave.done() == False:
                return False            
        return True
    
    def wait_tilldone(self):
        done = self.jobs_done()
        while done == False:
            sleep(SLEEPTIME)
            done = self.jobs_done()
            
class Slave:
    def __init__(self,bash_fn,walltime,cpu,mem,nodes,job_eo):
        self.walltime = walltime
        self.cpu = cpu
        self.mem = mem
        self.nodes = nodes
        self.bash_fn = bash_fn
        self.job_eo = job_eo
        self.id = None
        self.name = self.set_name()
        self.submit_job()

    def set_name(self):
        return "".join([str(randint(0,9)) for i in range(8)])

    def submit_job(self):
        command = "submitjob %s -c %s -queue low -J %s -m %s sh %s" % \
            (self.walltime,self.cpu,self.name,self.mem,self.bash_fn)
        system(command)
        self.set_id()

    def set_id(self):
        command = "bjobs -J %s" % self.name
        output = subprocess.check_output(command,shell=True)
        output = output.split('\n')[:-1]
        if len(output) != 2:
            sys.exit("Must exit, can't track job")
        output = output[-1]
        output = output.split()
        assert output[2] == self.name
        self.id = output[0]

    def done(self):
        command = "bjobs -J %s %s" % (self.name,self.id)
        output = subprocess.check_output(command,shell=True)
        status = output.split('\n')[1].split()[3]
        if status == "DONE":
            outerr = "%s/%s.OU" % (self.job_eo,self.id)
            with open(outerr,'r') as f:
                for line in f:
                    line = line.rstrip()
                    if line.startswith('Exited with exit code'):
                        sys.exit('One of your jobs failed, check %s' % outerr)
                    if line.startswith('Successfully completed'):
                        return True
        if status == "EXIT":
            sys.exit('Job was killed')
        return False

