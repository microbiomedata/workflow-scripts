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
import traceback
import jsonschema


class watcher():
    config = config()
    nmdc = nmdcapi()
    cromurl = config.conf['url']
    state_file = config.conf['agent_state']
    _POLL = 20
    _MAX_FAILS = 2

    _ALLOWED = ['metag-1.0.0', 'metat-1.0.0']
#    _ALLOWED = ['metag-1.0.0']

    def __init__(self):
        self.stage_dir = self.config.get_stage_dir()
        self.raw_dir = self.config.conf['raw_dir']
        self.jobs = []
        self.restored = False

    def restore(self, nocheck=False):
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
            jr = wfjob(state=job, nocheck=nocheck)
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
                self.nmdc.refresh_token()
                # Restore the state in case some other
                # process made a change
                self.restore()
                # Check for new jobs
                if not os.environ.get("SKIP_CLAIM"):
                    self.claim_jobs()
                # Check existing jobs
                self.nmdc.refresh_token()
                self.check_status()
             
                # Update op state
                # self.update_op_state()
            except Exception as e:
                print("Error")
                print(e)
                traceback.print_exc(file=sys.stdout)
            _sleep(self._POLL)

    def update_op_state(self, job):
        rec = self.nmdc.get_op(job.opid)
        if not rec['metadata'] or 'site_id' not in rec['metadata']:
            print("Corrupt op: %s" % (job.opid))
            # Botched record
            return None
        cur = rec['metadata'].get('extra')
        # Skip if nothing has changed
        if cur and cur['last_status']==job.last_status:
            return None
        print("updating %s" % (job.opid))
        md = job.get_state()
        res = self.nmdc.update_op(job.opid, done=job.done, meta=md)

    def update_op_state_all(self):
        for job in self.jobs:
            if job.opid:
                self.update_op_state(job)

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
        if 'object_id' not in rj['config']:
            # Legacy.  Skip.
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
        site = self.config.conf['site']
        self.jobs = []
        for op in self.nmdc.list_ops({"metadata.site_id": site}):
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
        wfid = njob['workflow']['id']
        if wfid.startswith('metag') or wfid.startswith('metat'):
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
            if not mdata['metadata']:
                # Silently skip these
                return
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
            print(j)
            jid = j['id']
            if j.get('claims') and len(j.get('claims')) > 0:
                    continue
            #jprint(j)
            print("try to claim:" + jid)
            self.nmdc.refresh_token()

            # claim job
            claim = self.nmdc.claim_job(jid)
            if not claim['claimed']:
                self.submit(j, claim['id'])
                self.ckpt()
#                sys.exit(1)
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

    def _fixup_activity(self, sdir, act):
        """
        Fix up some entries
        """
        # Cleanup
        if sdir == '.':
            # Drop this for now
            return None
        if 'part_of' in act:
            pof = act.get('part_of')
            act['part_of'] = [ pof ]
        if sdir == 'assembly' and 'filename' in act:
            act.pop('filename')
        if 'scaf_n_gt50k' in act:
            act['scaf_n_gt50K'] = act.pop('scaf_n_gt50k')
        if 'scaf_l_gt50k' in act:
            act['scaf_l_gt50K'] = act.pop('scaf_l_gt50k')
        # Drop Nulls in MAGs
        if sdir == 'MAGs' and 'mags_list' in act:
            for mag_list in act['mags_list']:
                for k, v in mag_list.items():
                    if v is None:
                        mag_list[k] = ""
        return act

    def generate_results(self, activity_id):
        dd = self.config.get_data_dir()
        outdir = os.path.join(dd, activity_id)
        results = {"data_object_set": []}
        if activity_id.startswith("nmdc:mga"):
            mp = {'.': 'activity_set',
                  'annotation': 'metagenome_annotation_activity_set',
                  'assembly': 'metagenome_assembly_set',
                  'MAGs': "mags_activity_set",
                  'qa': "read_QC_analysis_activity_set",
                  'ReadbasedAnalysis': "read_based_analysis_activity_set"
                 }
        elif activity_id.startswith("nmdc:mta"):
            mp = {'.': 'activity_set',
                  'annotation': 'metatranscriptome_annotation_activity_set',
                  'assembly': 'metatransciptome_assembly_set',
                  'qa': "read_QC_analysis_activity_set",
                  'metat_output': "metatranscriptome_activity_set"
                 }
        else:
            raise ValueError("Omics type not recongnized")
            
        for sdir, set_name in mp.items():
            fn = os.path.join(outdir, sdir, 'activity.json')
            act = self._load_json(fn)
            act = self._fixup_activity(sdir, act) 
            if act:
                results[set_name] = [act]
            if sdir != '.':
                fn = os.path.join(outdir, sdir, 'data_objects.json')
                dos = self._load_json(fn)
                for do in dos:
                    results['data_object_set'].append(do)
        schemafile =  os.environ.get("SCHEMA")
        if schemafile:
            schema = self._load_json(schemafile)
            try:
                jsonschema.validators.validate(results, schema)
            except jsonschema.exceptions.ValidationError as ex:
                print("Failed validation")
                print(ex)
                results = None
                sys.exit(1)
        return results

    def post_job_done(self, job):
        # Prepare the result record
        print("Running post for op %s" % (job.opid))
        dd = self.config.get_data_dir()
        outdir = os.path.join(dd, job.activity_id)
        results = {'activity_set': [], "data_object_set": []}
        for root, dirs, files in os.walk(outdir):
            for fn in files:
                if fn == 'activity.json':
                    path = os.path.join(root, fn)
                    d = self._load_json(path)
                    # TODO: This isn't in the schema yet.
                    if d['type'] == "nmdc:MetagenomeAnalysisActivity":
                        continue
                    results['activity_set'].append(d)
                elif fn == 'data_objects.json':
                    path = os.path.join(root, fn)
                    dos = self._load_json(path)
                    for do in dos:
                        results['data_object_set'].append(do)
        md = job.get_state()
        results_fn = os.path.join(outdir, 'results.json')
        with open(results_fn, "w") as f:
            f.write(json.dumps(results, indent=2))
        # TODO: Register this as an object as type
        job.done = True
        resp = self.nmdc.update_op(job.opid, done=True, results=results, meta=md)

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
            w.restore()
            for jobid in sys.argv[2:]:
                job = w.nmdc.get_job(jobid)
                claims = job['claims']
                if len(claims) == 0:
                    print("todo")
                    sys.exit(1)
                    claim = w.nmdc.claim_job(jobid)
                    opid = claim['detail']['id']
                else:
                    opid = claims[0]['op_id']
                    j = w.find_job_by_opid(opid)
                    if j:
                        print("%s use resubmit" % (jobid))
                        continue
                w.submit(job, opid, force=True)
                w.ckpt()
        elif sys.argv[1] == 'resubmit':
            # Let's do it by activity id
            w.restore()
            for val in sys.argv[2:]:
                job = None
                if val.startswith('gold'):
                    key = 'proj'
                elif val.startswith('Gp'):
                    key = 'proj'
                    val = "gold:" + val
                elif val.startswith('nmdc:sys'):
                    key = 'opid'
                else:
                    key = 'activity_id'
                for j in w.jobs:
                    jr = j.get_state()
                    if jr[key] == val:
                        job = j
                        break
                if not job:
                    print("No match found for %s" % (val))
                    continue
                if job.last_status in ["Running", "Submitted"]:
                    print("Skipping %s: %s" % (val, job.last_status))
                    continue
                job.cromwell_submit(force=True)
                jprint(job.get_state())
                w.ckpt()

        elif sys.argv[1] == 'fixckpt':
            w.fixckpt()
            w.ckpt()
        elif sys.argv[1] == 'dumpckpt':
            w.fixckpt()
            w.dumpckpt()
        elif sys.argv[1] == 'sync':
            w.restore()
            w.update_op_state_all()
        elif sys.argv[1] == 'daemon':
            w.watch() 
        elif sys.argv[1] == 'reset':
            print(w.nmdc.update_op(sys.argv[2], done=False))
        elif sys.argv[1] == 'results':
            # Given the activity ID
            for actid in sys.argv[2:]:
                jprint(w.generate_results(actid))
        elif sys.argv[1] == 'test':
            jprint(w.refresh_remote_jobs())


