#!/bin/env python
import subprocess
from os import environ,system
from os.path import isfile
from random import randint
import sys
from time import sleep

SLEEPTIME=60
WALLTIME=24
CPU=1
NODES=1

class Master:
    def __init__(self,terminate=True):
        self.alloc = environ.get('SJOB_DEFALLOC')
        if self.alloc == None:
            sys.exit('Initiate default allocation with ' 
                     'SJOB_DEFALLOC environment')
        self.job_eo = environ.get('SJOB_OUTPUT')
        if self.job_eo == None:
            self.job_eo = "%s/lsf-output" % environ['HOME']
        self.slaves = []
        self.terminate = terminate

    def add_job(self,bash_fn,walltime=WALLTIME,cpu=CPU,
                mem=CPU*2.5,nodes=NODES,architecture=None):
        my_slave = Slave(bash_fn,walltime,cpu,mem,nodes,self.job_eo,self.terminate,architecture)
        if my_slave == "KILL":
            self.kill_all_slaves()
        self.slaves.append(my_slave)

    def set_job(self):
        fn = "%s/%s.sh" % (self.job_eo,"".join([str(randint(0,9)) for i in range(8)]))
        while isfile(fn):
            fn = "%s/%s.sh" % (self.job_eo,"".join([str(randint(0,9)) for i in range(8)]))
        return fn

    def combine_jobs(self,fn,add_fn):
        with open(fn,'a') as f1:
            f1.write("sh %s\n" % add_fn)
            # with open(add_fn,'r') as f2:
            #     f1.write(f2.read())

    def jobs_done(self):
        for slave in self.slaves:
            status = slave.done()
            if status == False:
                return False
        return True

    def kill_all_slaves(self):
        for slave in self.slaves:
            command = "bkill %s >/dev/null" % slave.id
            system(command)
            sys.exit("Finished killing your jobs.. :)\nHave a nice day.")        
    
    def wait_tilldone(self):
        done = self.jobs_done()
        while done == False:
            sleep(SLEEPTIME)
            done = self.jobs_done()
            
class Slave:
    def __init__(self,bash_fn,walltime,cpu,mem,nodes,job_eo,terminate,architecture):
        self.walltime = walltime
        self.cpu = cpu
        self.mem = mem
        self.nodes = nodes
        self.bash_fn = bash_fn
        self.job_eo = job_eo
        self.id = None
        self.terminate = terminate
        self.architecture = architecture
        self.name = self.set_name()
        self.submit_job()

    def set_name(self):
        return "".join([str(randint(0,9)) for i in range(8)])

    def submit_job(self):
        # command = "submitjob %s -c %s -q private -g bashia02-04 -J %s -m %s -n %s sh %s" % \
        #     (self.walltime,self.cpu,self.name,self.mem,self.nodes,self.bash_fn)
        if self.architecture != None:
            command = "submitjob %s -c %s -a %s -q expressalloc -P acc_bashia02d -J %s -m %s -n %s sh %s >/dev/null" % \
                (self.walltime,self.cpu,self.architecture,self.name,self.mem,self.nodes,self.bash_fn)
        else:
            command = "submitjob %s -c %s -q expressalloc -P acc_bashia02d -J %s -m %s -n %s sh %s  >/dev/null" % \
            (self.walltime,self.cpu,self.name,self.mem,self.nodes,self.bash_fn)
        system(command)
        self.set_id()

    def set_id(self):
        command = "bjobs -J %s" % self.name
        output = subprocess.check_output(command,shell=True)
        output = output.split('\n')[:-1]
        if len(output) != 2:
            print "Must exit, can't track job"
            return "KILL"
        output = output[-1]
        output = output.split()
        assert output[2] == self.name
        self.id = output[0]

    # def done(self):
    #     command = "bjobs -J %s %s" % (self.name,self.id)
    #     output = subprocess.check_output(command,shell=True)
    #     if "EXIT" in output:
    #         outerr = "%s/%s.OU" % (self.job_eo,self.id)
    #         with open(outerr,'r') as f:
    #             for line in f:
    #                 line = line.rstrip()
    #                 if line.startswith('TERM_RUNLIMIT'):
    #                     sys.exit('JOB RAN OUT OF TIME, check %s' % outerr)
    #                 if line.startswith('TERM_MEMLIMIT'):
    #                     sys.exit('JOB RAN OUT OF MEM, check %s' % outerr)
    #         if self.terminate:
    #             sys.exit('Job was killed')
    #         else:
    #             print "Error but continuing check %s" % outerr 
    #             return True
    #     elif "PEND" in output:
    #         return False
    #     elif "RUN" in output:
    #         return False
    #     elif "DONE" in output:
    #         outerr = "%s/%s.OU" % (self.job_eo,self.id)
    #         with open(outerr,'r') as f:
    #             for line in f:
    #                 line = line.rstrip()
    #                 if line.startswith('Exited with exit code'):
    #                     sys.exit('One of your jobs failed, check %s' % outerr)
    #                 if line.startswith('Successfully completed'):
    #                     return True
    #     else:
    #         outerr = "%s/%s.OU" % (self.job_eo,self.id)
    #         if isfile(outerr):
    #             with open(outerr,'r') as f:
    #                 for line in f:
    #                     line = line.rstrip()
    #                     if line.startswith('Exited with exit code'):
    #                         sys.exit('One of your jobs failed, check %s' % outerr)
    #                     if line.startswith('Successfully completed'):
    #                         return True
    #         else:
    #             sys.exit('Something is wrong')

    def done(self):
        outerr = "%s/%s.OU" % (self.job_eo,self.id)
        if isfile(outerr):
            with open(outerr,'r') as f:
                for line in f:
                    line = line.rstrip()
                    if line.startswith('TERM_RUNLIMIT'):
                        print 'JOB RAN OUT OF TIME, check %s' % outerr
                        return True
                    elif line.startswith('TERM_MEMLIMIT'):
                        print 'JOB RAN OUT OF MEM, check %s' % outerr
                        return True
                    elif line.startswith('Exited with exit code'):
                        print 'One of your jobs failed, check %s' % outerr
                        return True
                    elif line.startswith('Successfully completed'):
                        return True
            print "Went through whole %s and did not find proper option" % outerr
            return True
        else:
            return False
