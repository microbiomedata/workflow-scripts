#!/usr/bin/env python

import pysub
import sys
import os
import json
from getpass import getuser
import requests
from nmdcapi import nmdcapi
from yaml import load
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

def not_implemented():
    raise OSError("Not Implemented")


class config():
    cf = os.path.join(os.environ['HOME'], '.wf_config')
    conf_file = os.environ.get('WF_CONFIG_FILE', cf)

    def __init__(self):
        
        self.conf = self._read_config()
        self.workflows = self._load_workflows()

    def _read_config(self):
        """
        Read config file for URL, WDL dir and template dir
        """
        conf = dict()
        if not os.path.exists(self.conf_file):
            sys.stderr.write("Missing %s.\n" % (self.conf_file))
            sys.stderr.write("Create or set WF_CONFIG_FILE\n")
            sys.exit(1)

        with open(self.conf_file) as f:
            for line in f:
                if line.startswith("#") or line == '\n':
                    continue
                (k, v) = line.rstrip().split('=')
                conf[k.lower()] = v
            if 'cromwell_url' not in conf:
                print("Missing URL")
                sys.exit(1)
            conf['url'] = conf['cromwell_url']
            if 'api' not in conf['url']:
                conf['url'] = conf['url'].rstrip('/') + "/api/workflows/v1"
        return conf

    def get_data_dir(self):
        return self.conf['data_dir']

    def get_stage_dir(self):
        return self.conf['stage_dir']

    def _load_workflows(self):
        """
        Read in the workflow attributes from a yaml file.
        """
        return load(open(self.conf['workflows']), Loader=Loader)


class job():
    nmdc = nmdcapi()
    config = config()
    conf = config.conf
    cromurl = conf['url']
    data_dir = conf['data_dir']
    resource = conf['resource']
    url_root = conf['url_root']
    debug = False

    def __init__(self, fn=None, typ=None, nmdc_jobid=None, proj=None, opid=None, activity_id=None, state=None):
        if state:
            self.activity_id = state['activity_id']
            self.nmdc_jobid = state['nmdc_jobid']
            self.opid = state.get('opid', None)
            self.type = state['type']
            self.proj = state['proj']
            self.jobid = state['cromwell_jobid']
            self.last_status = state['last_status']
            self.fn = state['fn']
            self.failed_count = state.get('failed_count', 0)
            self.done = state.get('done', None)
        else:
            self.activity_id = activity_id
            self.type = typ
            self.proj = proj
            self.nmdc_jobid = nmdc_jobid
            self.opid = opid
            self.done = None
            self.fn = fn
            self.jobid = None
            self.failed_count = 0
            self.last_status = "Unsubmitted"
        # Set workflow parameters
        wf = self.config.workflows[self.type]
        self.workflow = self.config.workflows[self.type]
        self.version = self.workflow["version"]
        self.pipeline = self.workflow["pipeline"]
        self.git_url = self.workflow["git_url"]
        self.prefix = self.workflow["prefix"]

        if not self.activity_id:
            prefix = wf["prefix"]
            self.activity_id = self.nmdc.mint("nmdc", prefix, 1)[0]

        if self.jobid:
            self.check_status()

        if self.opid:
            opstat = self.nmdc.get_op(self.opid)
            self.done = opstat['done']

    def get_state(self):
        data = {
                "type": self.type,
                "cromwell_jobid": self.jobid,
                "nmdc_jobid": self.nmdc_jobid,
                "proj": self.proj,
                "activity_id": self.activity_id,
                "last_status": self.last_status,
                "done": self.done,
                "fn": self.fn,
                "failed_count": self.failed_count,
                "opid": self.opid
                }
        return data

    def log(self, msg):
        if self.debug:
            print(msg)

    def _generate_label(self):
        """
        Generate a label
        """
        label = {
                 "pipeline_version": self.version,
                 "pipeline": self.pipeline,
                 "project_id": self.proj,
                 "activity_id": self.activity_id,
                 "submitter": getuser()
                }
        self.label = label
        return label

    def _generate_input(self):
        """
        Generate input file for cromwell.
        """
        data_dir = os.path.join(self.data_dir, self.activity_id)
            
        # TODO:make some type of template
        if self.type == "Metagenome":
            inp = {
                   "nmdc_metag.proj": self.activity_id,
                   "nmdc_metag.informed_by": self.proj,
                   "nmdc_metag.input_file": self.fn,
                   "nmdc_metag.git_url": self.git_url,
                   "nmdc_metag.outdir": data_dir,
                   "nmdc_metag.resource": self.resource,
                   "nmdc_metag.url_root": self.url_root
                 }
        elif self.type == "Metatranscriptome":
            inp = {
                   "nmdc_metat.proj":  self.activity_id,
                   "nmdc_metat.informed_by": self.proj,
                   "nmdc_metat.input_file": self.fn,
                   "nmdc_metat.git_url": self.git_url,
                   "nmdc_metat.outdir": data_dir,
                   "nmdc_metat.resource": self.resource,
                   "nmdc_metat.url_root": self.url_root
                  }
        else:
            raise ValueError("Huh? What is %s" % (self.type))

        self.input = inp
        return inp

    def check_status(self):
        """
        Check the status in Cromwell
        """
        if not self.jobid:
            return "Unsubmitted"
        url = "%s/%s/status" %(self.cromurl, self.jobid)
        resp = requests.get(url)
        state = "Unknown"
        if resp.status_code == 200:
            data = resp.json()
            state = data['status']
        self.last_status = state
        return state

    def cromwell_submit(self, force=False):
        """
        Check if a task needs to be submitted.
        """

        # Refresh the log
        status = self.check_status()
        if not force and status not in ['Failed', 'Aborted', 'Aborting', "Unsubmitted"]:
            self.log("Skipping: %s %s" % (self.fn, status))
            return
        # Reuse the ID from before
        self.log("Resubmit %s" % (self.activity_id))
        self._generate_label()
        self._generate_input()
        jid = pysub.ezsubmit(self.cromurl, self.workflow['wdl'], self.conf['wdl_dir'],
                             self.input, labels=self.label, bundle_fn=self.workflow['bundle'])
        print(jid)
        self.jobid = jid

