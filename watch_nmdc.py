#!/usr/bin/env python

from nmdcapi import nmdcapi
from wfutils import config, job as wfjob
from pysub import read_config
from time import sleep as _sleep
import jwt
import json
import os
import sys
import requests
import shutil


class watcher():
    config = config()
    nmdc = nmdcapi()
    cromurl = config.conf['url']
    state_file = config.conf['agent_state']
    _POLL = 20
    _MAX_FAILS = 2

    _ALLOWED = ['metag-1.0.0']

    def __init__(self):
        self.stage_dir = self.config.get_stage_dir()
        self.raw_dir = self.config.conf['raw_dir']
        self.jobs = []
        self.restored = False

    def restore(self):
        """
        Restore from chkpt
        """
        data = json.loads(open(self.state_file).read())
        new_job_list = []
        seen = dict()
        for job in data['jobs']:
            jid = job['nmdc_jobid']
            if jid in seen:
                continue
            jr = wfjob(state=job)
            new_job_list.append(jr)
            seen[jid] = True
        self.jobs = new_job_list
        self.restored = True

    def dumpckpt(self):
        jobs = []
        for job in self.jobs:
            jobs.append(job.get_state())
        data = {'jobs': jobs}
        print(json.dumps(data, indent=2))

    def ckpt(self):
        jobs = []
        for job in self.jobs:
            jobs.append(job.get_state())
        data = {'jobs': jobs}
        with open(self.state_file, "w") as f:
            f.write(json.dumps(data, indent=2))

    def watch(self):
        """
        The endless loop
        """
        print("Entering polling loop")
        while True:
            try:
                # Restore the state in case some other
                # process made a change
                self.restore()
                # Check for new jobs
                self.claim_jobs()
                # Check existing jobs
                self.check_status()
             
                # Update op state
                # self.update_op_state()
            except Exception as e:
                print("Error")
                print(e)
            _sleep(self._POLL)

    def update_op_state(self):
        for job in self.jobs:
            if job.opid:
                rec = self.nmdc.get_op(job.opid)
                if not rec['metadata'] or 'site_id' not in rec['metadata']:
                    print("Corrupt op: %s" % (job.opid))
                    # Botched record
                    continue
                cur = rec['metadata'].get('extra')
                # Skip if nothing has changed
                if cur and cur['last_status']==job.last_status:
                    continue
                print("updating %s" % (job.opid))
                md = job.get_state()
                res = self.nmdc.update_op(job.opid, meta=md)

    def cromwell_list_jobs_label(self, key, value):
        query = "label={}:{}&additionalQueryResultFields=labels".format(key, value)
        url = "%s/query?%s" % (self.cromurl, query)
        resp = requests.get(url)
        d = resp.json()
        return d

    def reconstruct_state(self, op):
        if 'job' not in op['metadata']:
            return None
        nmdc_jobid = op['metadata']['job']['id']
        # This is the remote job rec
        rj = op['metadata']['job']
        if rj['workflow']['id'] not in self._ALLOWED:
            return None
        # This is the input object id
        inp = rj['config']['object_id']
        obj = self.nmdc.get_object(inp, decode=True)
        url = obj['access_methods'][0]['access_url']['url']
        fn = url.split('/')[-1]
        dest = os.path.join(self.stage_dir, fn)
        mdata = obj['metadata']
        proj = mdata['proj']
        typ = mdata['type']
        # Let's try to figure out the last cromwell job
        # that ran for this data.
        cjobs = self.cromwell_list_jobs_label('project_id', proj)
        for cj in cjobs['results']:
            # If it doesn't have an activity record it is old
            act_id = cj['labels'].get('activity_id')
            if not act_id:
                continue
            # If the activity ID has this, then they are old
            if act_id.startswith('nmdc:mg0') or act_id.startswith('nmdc.mt0'):
                act_id = None
                continue
            break
        jstate =  {
                "nmdc_jobid": nmdc_jobid,
                "opid": op['id'],
                "done": op['done'],
                "input": inp,
                "type": typ,
                "activity_id": act_id,
                "fn": dest,
                "cromwell_jobid": cj['id'],
                "last_status": cj['status'],
                "proj": proj
                }
        return jstate

    def fixckpt(self):
        # Try to fix up ckpt state from multiple 
        # Sources
        self.jobs = []
        for op in self.nmdc.list_ops():
            jstate = self.reconstruct_state(op) 
            if jstate:
                newjob = wfjob(state=jstate)
                self.jobs.append(newjob)
        return

    # Could be moved into job area
    def stage_data(self, url):
        """
        Stage the data files
        """
        fn = url.split('/')[-1]
        dst = os.path.join(self.stage_dir, fn)
        if os.path.exists(dst):
            print("File staged")
            return dst
        src = os.path.join(self.raw_dir, fn)
        tmp = dst + ".tmp"
        if os.path.exists(src):
            # Already cached locally so just copy
            shutil.copyfile(src, tmp)
            os.rename(tmp, dst)
        else:
            resp = requests.get(url) 
            with open(tmp, "w") as f:
                f.write(resp.text)
            os.rename(tmp, dst)
        return dst

    def find_job_by_opid(self, opid):
        for j in self.jobs:
            if j.opid == opid:
                return j
        return None


    def submit(self, njob, opid, force=False):
        if njob['workflow']['id'].startswith('metag'):
            # Collect some info from the object
            if 'object_id_latest' in njob['config']:
                print("Old record. Skipping.")
                return 
            elif 'object_id' in njob['config']:
                inp = njob['config']['object_id']
            else:
                print(njob)
                sys.stderr.write("missing input")
                return
            mdata = self.nmdc.get_object(inp, decode=True)
            typ = mdata['metadata']['type']
            proj = mdata['metadata']['proj']

            # Stage file
            url = mdata['access_methods'][0]['access_url']['url']
            fn = self.stage_data(url)
            job = self.find_job_by_opid(opid)
            activity_id = None
            if job:
                print("Previously cached job")
                print("Reusing activity %s" % (job.activity_id))
                activity_id = job.activity_id
            else:
                # Create a new job
                job = wfjob(fn, typ, njob['id'], proj, opid)
                self.jobs.append(job)
            job.cromwell_submit(force=False)
        else:
            print("Type not recongnized %s" % (njob['workflow']['id']))

    def refresh_remote_jobs(self):
        """
        Return a filtered list of nmdc jobs.
        """
        filt = {"workflow.id": {"$in": self._ALLOWED}}
        jobs = self.nmdc.list_jobs(filt=filt)
        # Get the jobs we know about
        known = dict()
        for j in self.jobs:
            known[j.nmdc_jobid] = 1
        resp = []
        for j in jobs:
            jid = j['id']
#            if j['workflow']['id'] not in self._ALLOWED:
#                continue
            if jid in known:
                continue
            resp.append(j)
        return resp


    def claim_jobs(self):
        for j in self.refresh_remote_jobs():
            jid = j['id']
            print("try to claim:" + jid)

            # claim job
            claim = self.nmdc.claim_job(jid)
            if not claim['claimed']:
                self.submit(j, claim['id'])
                self.ckpt()
            else:
                # Previously claimed
                opid = claim['detail']['id']
                op = self.nmdc.get_op(opid)
                print("Previously claimed.")
                print(opid)
                self.submit(j, opid)
                self.ckpt()

    def _load_json(self, fn):
        return json.loads(open(fn).read())

    def post_job_done(self, job):
        # Prepare the result record
        dd = self.config.get_data_dir()
        outdir = os.path.join(dd, job.activity_id)
        results = {'activities': [], "data_objects": []}
        for root, dirs, files in os.walk(outdir):
            for fn in files:
                if fn == 'activity.json':
                    path = os.path.join(root, fn)
                    results['activities'].append(self._load_json(path))
                elif fn == 'data_objects.json':
                    path = os.path.join(root, fn)
                    results['data_objects'].append(self._load_json(path))
        resp = self.nmdc.update_op(job.opid, done=True, results=results)
        job.done = True

    def check_status(self):
        for job in self.jobs:
            if job.done:
                continue
            status = job.check_status()
            if status == 'Succeeded' and job.opid and not job.done:
                self.post_job_done(job)
                self.ckpt()
            elif status == 'Failed' and job.opid:
                if job.failed_count < self._MAX_FAILS:
                    job.failed_count += 1
                    job.cromwell_submit()
                self.ckpt()
            elif job.opid and not job.done:
                continue
                print("%s op:%s: crom: %s %s" % (job.nmdc_jobid,
                                                       job.opid,
                                                       job.jobid,
                                                       status))

def jprint(obj):
    print(json.dumps(obj, indent=2))


if __name__ == "__main__":
    w = watcher()
    if len(sys.argv) > 1:
        # Manual mode
        if sys.argv[1] == 'submit':
            jobid = sys.argv[2]
            w.restore()
            job = w.nmdc.get_job(jobid)
            claim = w.nmdc.claim_job(jobid)
            if claim['claimed']:
                opid = claim['detail']['id']
                print(opid)
            w.submit(job, opid, force=True)
            w.ckpt()
        elif sys.argv[1] == 'resubmit':
            # Let's do it by activity id
            actid = sys.argv[2]
            w.restore()
            job = None
            for j in w.jobs:
                if j.activity_id == actid:
                    job = j
                    break
            if not job:
                print("No match found")
            else:
                job.cromwell_submit(force=True)
                jprint(job.get_state())

        elif sys.argv[1] == 'fixckpt':
            w.fixckpt()
            w.ckpt()
        elif sys.argv[1] == 'dumpckpt':
            w.fixckpt()
            w.dumpckpt()
        elif sys.argv[1] == 'sync':
            w.restore()
            w.update_op_state()
        elif sys.argv[1] == 'watch':
            w.watch() 
        elif sys.argv[1] == 'reset':
            print(w.nmdc.update_op(sys.argv[2], done=False))

