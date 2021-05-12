#!/usr/bin/env python

from time import sleep as _sleep
from threading import Thread
from subprocess import Popen, PIPE as _PIPE, run
from select import select
import json
import sys
import time

POLL_TIME = 120
_MAX_NODES = 55

# PARAMS
_USE_FAST = 25
_Q_FAST = 'interactive'
_Q_FAST_TIME = '4:00:00'
_Q_FAST_MAX_NODES = 25
_Q_REGULAR = 'regular'
_Q_REGULAR_TIME = '12:00:00'
_Q_FAST_MAX_JOBS =2
_SUBMIT_SCRIPT = '/global/homes/n/nmdcda/condor_worker_idle.sh'
_JOB_NAME = 'nmdc_condor_wrk'
_LOG_FILE = 'autoscale.log'

def backlog():
    """
    Query Cromwelll to get current number of pending jobs.  Just a
    straight count for now.
    """

    # 0 Unexpanded  U
    # 1   Idle    I
    # 2   Running R
    # 3   Removed X
    # 4   Completed   C
    # 5   Held    H
    # 6   Submission_err  E

    # Let's query pending and running jobs to get the total current demand
    cmd = ['condor_q', '-constraint', 'JobStatus == 1 || JobStatus == 2', '-json']
    proc = Popen(cmd, stdout=_PIPE, stderr=_PIPE)
    raw = ''
    for line in iter(proc.stdout.readline,''):
        if not line:
            break
        raw+=line.decode('utf-8')
    rv = proc.wait()
    jobs = []
    try:
        if raw:
           jobs = json.loads(raw)
    except:
        print("bad message")
        print(raw)
        return None
    return len(jobs)

def _convert_to_hours(s_time):
    """
    Convert slurm time to a float hours.
    """
    days = 0.0
    hours_str= s_time
    if '-' in s_time:
       (day, hours_str) = s_time.split('-')
       days = float(day)
    ele = hours_str.split(':')
    if len(ele) > 2:
        (h, m, _) = ele
    elif len(ele) > 1:
        h = 0
        (m, _) = ele
    else:
        return 0
    return days*24 + float(h) + float(m)/60

def slurm():
    """
    Query slurm to get list of running and pending jobs.  Filters on
    _JOB_NAME for the job name.
    """
    # JobId   JobName State  Nodes  RequestedTime  RemainingTime
    cmd = ['squeue', '-u', 'nmdcda', '--noheader', '-o', '%A\t%j\t%t\t%D\t%l\t%L\t%P']

    proc = Popen(cmd, bufsize=600*1024*1024, stdout=_PIPE, stderr=_PIPE)
    jobs = []
    for line in iter(proc.stdout.readline,''):
        if not line:
            break
        (job_id, job_name, state, nodes, requested_time, remaining_time, part) = line.decode('utf-8').rstrip().split('\t')
        q = part.split('_')[0]
        if _JOB_NAME not in job_name:
            continue
        jrec = {
                'job_id': job_id,
                'job_name': job_name,
                'state': state,
                'nodes': int(nodes),
                'queue': q,
                'requested_time': _convert_to_hours(requested_time),
                'remaining_time': _convert_to_hours(remaining_time)
                }
        jobs.append(jrec)
       
    rv = proc.wait()
    return jobs

def capacity():
    workers = slurm()
    running_nodes = 0
    running_time = 0
    pending_nodes = 0
    pending_time = 0
    fast_q = 0
    for worker in workers:
        if worker['queue'].startswith(_Q_FAST):
            fast_q += 1
        if worker['state'] == 'R':
            running_nodes += worker['nodes']
            running_time += worker['remaining_time']
        else:
            pending_nodes += worker['nodes']
            pending_time += worker['remaining_time']
    resp = {
            'workers': workers,
            'fast_q': fast_q,
            'running_nodes': running_nodes,
            'running_time': running_time,
            'pending_nodes': pending_nodes,
            'pending_time': pending_time,
            'total_nodes': running_nodes + pending_nodes,
            'total_time': running_time + pending_time
            }
    return resp

def schedule(bcklog, cap):
    """
    This should look at the state and return how much things should change.
    """
    total_nodes = cap['total_nodes']
    return bcklog - total_nodes

def _readio(p):
    cont = True
    last = False
    while cont:
        rlist = [p.stdout, p.stderr]
        x = select(rlist, [], [], 1)[0]
        for f in x:
            line = f.readline().decode('utf-8')
            if f == p.stderr:
                sys.stderr.write(line)
            else:
                sys.stdout.write(line)
        if last:
            cont = False
        if p.poll() is not None:
            last = True

def submit(nodes, q, t):
    print("submit %d nodes to %s" % (nodes, q))
    #cmd = ['sbatch', '-J', _JOB_NAME, '-C', 'haswell', '-q', q, '-N', str(nodes), _SUBMIT_SCRIPT]
    cmd = ['srun', '--pty', '-J', _JOB_NAME, '-C', 'haswell', 
           '-q', q, '-N', str(nodes), '-t', t, _SUBMIT_SCRIPT]
    print(cmd)
    proc = Popen(cmd, bufsize=0, stdout=_PIPE, stderr=_PIPE)
    out = Thread(target=_readio, args=[proc], daemon=True)
    out.start()

def cancel(jobid):
    print("canceling %s" % (jobid))
    #cmd = ['sbatch', '-J', _JOB_NAME, '-C', 'haswell', '-q', q, '-N', str(nodes), _SUBMIT_SCRIPT]
    cmd = ['scancel', jobid]
    try:
        res = run(cmd, capture_output=True)
        if res:
            print("Failed to cancel job")
    except Exception:
        print("Failed to cancel job")


def grow(nodes, cap, do_submit=True):
    """
    Grow the cluster by submitting slurm jobs.
    Considerations:
    Use some fast queue when things are pretty idle.
    """
    q = _Q_REGULAR
    t = _Q_REGULAR_TIME
    n = nodes
    if cap['total_nodes'] >= _MAX_NODES:
        n = 0
    if cap['running_nodes'] < _USE_FAST and \
       cap['fast_q'] < _Q_FAST_MAX_JOBS:
        q = _Q_FAST
        t = _Q_FAST_TIME
        n = min(_Q_FAST_MAX_NODES, nodes)
    else:
        n = min(_MAX_NODES - cap['total_nodes'], nodes)
    if do_submit and n > 0:
        submit(n, q, t)
    else:
        return (n, q, t)
         
def log_state(jobs, cap, nodes):
    with open(_LOG_FILE, 'a') as f:
        f.write('%s\n' % (str(time.time())))
        f.write('jobs: %d\n' % (jobs))
        f.write('cap:\n%s\n' % (str(cap)))
        f.write('nodes: %d\n\n' % (nodes))

def shrink(nodes, cap):
    """
    Shrink the cluster by canceling jobs or scontrol updates (TODO)
    Considerations:
    Wait some idle time (~10 minutes before shrinking things)
    """

    # Total Idle...let's kill things
    print("Shrink required... not implemented")
    # Check for any pending jobs first.
    for job in cap['workers']:
        if job['state'] == 'PD':
            cancel(job['job_id'])
#        {'job_id': '41608336', 'job_name': 'nmdc_condor_wrk', 'state': 'PD', 'nodes': 50, 'queue': 'regular', 'requested_time': 12.0, 'remaining_time': 12.0}

    return

if __name__ == "__main__":
    cont = True
    while cont:
        print("polling")
        jobs = backlog()
        if jobs is None:
            _sleep(30)
            continue
        cap = capacity()
        nodes = schedule(jobs, cap)
        if nodes:
            log_state(jobs, cap, nodes)
        if nodes > 0:
            grow(nodes, cap)
        elif nodes < 0:
            shrink(-nodes, cap)
        if nodes:
            log_state(jobs, cap, nodes)
        _sleep(POLL_TIME)


def test_schedule():
    cap = {
            'workers': [],
            'running_nodes': 0,
            'running_time': 0,
            'pending_nodes': 0,
            'pending_time': 0,
            'total_nodes': 0,
            'total_time': 0
            }
    # Empty with 1 job
    res = schedule(1, cap)
    assert res==1

    # Matched size
    cap['total_nodes'] = 1
    cap['total_time'] = 24*12
    res = schedule(1, cap)
    assert res==0


def test_grow():
    # Initialize things
    cap = {
            'workers': [],
            'fast_q': 0,
            'running_nodes': 0,
            'running_time': 0,
            'pending_nodes': 0,
            'pending_time': 0,
            'total_nodes': 0,
            'total_time': 0
            }

    # Max size
    cap['total_nodes'] = _MAX_NODES
    cap['total_time'] = _MAX_NODES*24*12
    (n, q, t) = grow(_MAX_NODES, cap, do_submit=False)
    assert n==0

    # Max size Idle
    cap['total_nodes'] = 0
    cap['total_time'] = 0
    (n, q, t) = grow(_MAX_NODES, cap, do_submit=False)
    assert n==_Q_FAST_MAX_NODES
    assert q==_Q_FAST
    assert t==_Q_FAST_TIME

    # 2xMax demand, max size, max fastq
    n_nodes = int(_MAX_NODES/2)
    cap['fast_q'] = _Q_FAST_MAX_JOBS
    cap['total_nodes'] = n_nodes
    cap['total_time'] = n_nodes*24*12
    (n, q, t) = grow(_MAX_NODES*2, cap, do_submit=False)
    assert n==_MAX_NODES-n_nodes

    # 2xMax demand, 5 less than Max, max fastq
    t_nodes = _MAX_NODES-5
    cap['total_nodes'] = t_nodes
    cap['total_time'] = t_nodes*24*12
    (n, q, t) = grow(_MAX_NODES*2, cap, do_submit=False)
    assert n==5
    assert q==_Q_REGULAR

# TODO
def xtest_shrink():
    # Max size-1, full
    n = _MAX_NODES
    cap['total_nodes'] = n
    cap['total_time'] = n*24*12
    res = schedule(_MAX_NODES-1, cap)
    assert res == -1

    # Max size half full
    n = _MAX_NODES
    cap['total_nodes'] = n
    cap['total_time'] = n*24*12
    res = schedule(0, cap)
    assert res == -_MAX_NODES


def test_log():
    cap = {
            'workers': [{'bogus': 1}],
            'running_nodes': 1,
            'running_time': 24,
            'pending_nodes': 4,
            'pending_time': 48,
            'total_nodes': 5,
            'total_time': 72
            }
    log_state(10, cap, 1)
