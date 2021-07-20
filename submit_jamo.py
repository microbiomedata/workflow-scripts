#!/usr/bin/env python

import pysub
import sys
import os
import json
from getpass import getuser
import requests
from yaml import load
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

"""
This reads a JAMO metadata record and submits an
end-to-end annotation job.
"""

def load_workflows():
    """
    Read in the workflow attributes from a yaml file.
    """
    return load(open('wf_configs.yaml'), Loader=Loader)

def read_meta(fn, workflows):
    """
    Read the metadata file and return back a normalized structure.
    """
    with open(fn) as f:
        md = json.loads(f.read())
    prod = md['metadata']['sequencing_project']['sequencing_product_name']
    typ = workflows["Product Mappings"][prod]
    data = {
            "type": typ,
            "file": fn.replace('.json', ''),
            "file_size": md['file_size'],
            "proj": 'gold:' + md['metadata']['gold_data']['gold_stamp_id'],
            "version": workflows[typ]["version"],
            "pipeline": workflows[typ]["pipeline"],
            "git_url": workflows[typ]["git_url"],
            "prefix": workflows[typ]["prefix"]
         }
    return data

def generate_label(md):
    """
    Generate a label
    """
    label = {
             "pipeline_version": md['version'],
             "pipeline": md['pipeline'],
             "project_id": md['proj'],
             "activity_id": md['activity_id'],
             "submitter": getuser()
            }
    return label

def generate_input(conf, md):
    """
    Generate input file for cromwell.
    """
    data_dir = os.path.join(conf['data_dir'], md['activity_id'])
        
    # TODO:make some type of template
    inp = {
           "nmdc_metag.proj": md['activity_id'],
           "nmdc_metag.informed_by": md['proj'],
           "nmdc_metag.input_file": md['file'],
           "nmdc_metag.git_url": md['git_url'],
           "nmdc_metag.outdir": data_dir,
           "nmdc_metag.resource": conf['resource'],
           "nmdc_metag.url_root": conf['url_root']
         }

    return inp


def get_activity_id(conf, prefix):
    # This is temporary hack
    sf = conf['activity_id_state']
    actid = int(open(sf).read())
    actid += 1
    with open(sf, "w") as f:
        f.write("%d\n" % (actid))
    return "nmdc:%s%07d" % (prefix, actid)

def check_status(base, job):
    if job=="dryrun":
        print("Dry-run: redoing")
        return False
    url = "%s/%s/status" %(base, job)
    resp = requests.get(url, verify=False)
    state = "Unknown"
    if resp.status_code == 200:
        data = resp.json()
        state = data['status']
    else:
        print(resp)

    return state

def write_log(fn_sub, inp, lbl, jid):
    data = {
            "input": inp,
            "labels": lbl,
            "jobid": jid
            }
    with open(fn_sub, "w") as f:
        f.write(json.dumps(data, indent=2)) 

def submit(conf, md, wf, dryrun=False, verbose=False):
    fn_sub = md['file'] + '.sublog'
    if os.path.exists(fn_sub):
        sublog = json.loads(open(fn_sub).read())
        if verbose:
            print(json.dumps(sublog, indent=2))
        status = check_status(conf['url'], sublog['jobid'])
        if status not in ['Failed', 'Aborted', 'Aborting']:
            print("Skipping: %s %s" % (md['file'], status))
            return
        # Reuse the ID from before
        md['activity_id'] = sublog['input']['nmdc_metag.proj']
        print("Resubmit %s" % (md['activity_id']))
    else:
        md['activity_id'] = get_activity_id(conf, md['prefix'])
    lbl = generate_label(md)
    inp = generate_input(conf, md)
    jid = pysub.ezsubmit(conf['url'], wf['wdl'], conf['wdl_dir'], inp, labels=lbl, bundle_fn=wf['bundle'], dryrun=dryrun)
    if not dryrun:
        write_log(fn_sub, inp, lbl, jid)

if __name__ == "__main__":
    conf = pysub.read_config()
    workflows = load_workflows()
    idx = 1
    dryrun = False
    verbose = False
    if '-n' in sys.argv:
        print("Dryrun")
        idx += 1
        dryrun = True
    if '-v' in sys.argv:
        print("Verbose")
        idx += 1
        verbose = True
    for fn in sys.argv[idx:]:
        if not fn.endswith('.json'):
            fn = fn + '.json'
        md = read_meta(fn, workflows)
        typ = md['type']
        wf = workflows[typ]
        if typ != "Metagenome":
            print("TODO: Add support for non-Metagenome workflows")
            continue
        submit(conf, md, wf, dryrun=dryrun, verbose=verbose)
         
