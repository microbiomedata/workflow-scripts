#!/usr/bin/python

"""
monitor a workers business and shut it down if it is idle.

"""

from time import sleep as _sleep
from threading import Thread
from subprocess import Popen, PIPE as _PIPE
from select import select
import json
import os
import sys
import time
import socket
from signal import SIGINT

POLL_TIME = 30
# Max idle in seconds
MAX_IDLE_TIME = 180

def _run(cmd):
    proc = Popen(cmd, stdout=_PIPE, stderr=_PIPE)
    raw = ''
    for line in iter(proc.stdout.readline,''):
        if not line:
            break
        raw+=line.decode('utf-8')
    rv = proc.wait()
    return raw, rv

def get_status(hostname):
   # Let's query pending and running jobs to get the total current demand
    cmd = ['condor_status', '-json']
    proc = Popen(cmd, stdout=_PIPE, stderr=_PIPE)
    raw = ''
    for line in iter(proc.stdout.readline,''):
        if not line:
            break
        raw+=line.decode('utf-8')
    rv = proc.wait()
    status = []
    try:
        if raw:
           status = json.loads(raw)
    except:
        print("bad message")
        print(raw)
        return None
    default = None
    for n in status:
        if n['Name'] == hostname:
            return n
        name = n['Name'].split('@')[-1]
        if name == hostname:
            default = n
    return default

def cull(hostname, job):
    print('cull')
    os.kill(int(os.environ['MASTER_PID']), SIGINT)
    # Get the node list
    retry = True
    while retry:
        nidlist_comp, rv = _run(['squeue', '-w', hostname, '-o', '%N', '-h'])
        # Expand 
        nidlist_str, rv = _run(['scontrol', 'show', 'hostnames', nidlist_comp.rstrip()])
        nidlst = nidlist_str.rstrip().split('\n')
        if hostname not in nidlst:
            # Already removed
            break
        nidlst.remove(hostname)
        print(nidlst)
        if len(nidlst)==0:
            print("Only node left...exiting")
            sys.exit()
        result, rv = _run(['scontrol', 'update', 
                       'job', job, 'nodelist', ','.join(nidlst)])
        print(result)
        _sleep(1)
    # Wait to die
   

def main():
    hostname = socket.gethostname()
#    hostname = 'nid00103'
    idle_time = 0
    job = os.environ['SLURM_JOBID']

    while True:
        status = get_status(hostname)
        if not status:
            print("Hostname not found...maybe already exited")
            sys.exit(1)
        state = status['Activity']
        if state == 'Idle':
            idle_time += POLL_TIME
        else:
            idle_time = 0
        if idle_time > MAX_IDLE_TIME:
#            print("Max Idle...Exiting")
#            sys.exit()
            cull(hostname, job)
        _sleep(POLL_TIME)


if __name__ == '__main__':
    main()
