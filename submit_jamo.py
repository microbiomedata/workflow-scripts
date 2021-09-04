#!/usr/bin/env python

import pysub
import sys
import os
import json
from getpass import getuser
import requests
from nmdcapi import get_token, mint
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
    typ = md['type']
    if typ == "Metagenome":
        inp = {
               "nmdc_metag.proj": md['activity_id'],
               "nmdc_metag.informed_by": md['proj'],
               "nmdc_metag.input_file": md['file'],
               "nmdc_metag.git_url": md['git_url'],
               "nmdc_metag.outdir": data_dir,
               "nmdc_metag.resource": conf['resource'],
               "nmdc_metag.url_root": conf['url_root']
             }
    elif typ == "Metatranscriptome":
        inp = {
               "nmdc_metat.proj":  md['activity_id'],
               "nmdc_metat.informed_by": md['proj'],
               "nmdc_metat.input_file": md['file'],
               "nmdc_metat.git_url": md['git_url'],
               "nmdc_metat.outdir": data_dir,
               "nmdc_metat.resource": conf['resource'],
               "nmdc_metat.url_root": conf['url_root']
              }
    else:
        raise ValueError("Huh? What is %s" % (typ))

    return inp


def get_activity_id(conf, prefix):
    tok = get_token()
    actid = mint(tok, "nmdc", prefix, 1)[0]
    print(actid)
    # This is temporary hack
    #sf = conf['activity_id_state']
    #actid = int(open(sf).read())
    #actid += 1
    #with open(sf, "w") as f:
    #    f.write("%d\n" % (actid))
    return actid

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

def write_log(fn_sub, inp, lbl, jid, typ, actid):
    data = {
            "input": inp,
            "labels": lbl,
            "type": typ,
            "jobid": jid,
            "activity_id": actid
            }
    with open(fn_sub, "w") as f:
        f.write(json.dumps(data, indent=2))

def read_log(fn_sub):
    sublog = None
    if os.path.exists(fn_sub):
        sublog = json.loads(open(fn_sub).read())
    return sublog

def submit(conf, md, wf, dryrun=False, verbose=False, force=False):
    """
    Check if a task needs to be submitted.
    """
    fn_sub = md['file'] + '.sublog'
    sublog = read_log(fn_sub)
    if sublog:
        if verbose:
            print(json.dumps(sublog, indent=2))
        status = check_status(conf['url'], sublog['jobid'])
        if not force and status not in ['Failed', 'Aborted', 'Aborting']:
            print("Skipping: %s %s" % (md['file'], status))
            return
        # Reuse the ID from before
        # TODO: Maybe include in the wf config yaml
        if md['type'] == 'Metagenome': 
            prev_proj =  sublog['input']['nmdc_metag.proj']
        else:
            prev_proj =  sublog['input']['nmdc_metat.proj']
        md['activity_id'] = prev_proj
        print("Resubmit %s" % (md['activity_id']))
    else:
        md['activity_id'] = get_activity_id(conf, md['prefix'])
    lbl = generate_label(md)
    inp = generate_input(conf, md)
    jid = pysub.ezsubmit(conf['url'], wf['wdl'], conf['wdl_dir'], inp, labels=lbl, bundle_fn=wf['bundle'], dryrun=dryrun)
    if not dryrun:
        write_log(fn_sub, inp, lbl, jid, md['type'], md['activity_id']) 

if __name__ == "__main__":
    conf = pysub.read_config()
    workflows = load_workflows()
    idx = 1
    dryrun = False
    verbose = False
    force = False
    if '-n' in sys.argv:
        print("Dryrun")
        idx += 1
        dryrun = True
    if '-v' in sys.argv:
        print("Verbose")
        idx += 1
        verbose = True
    if '-f' in sys.argv:
        print("Force")
        idx += 1
        force = True
    for fn in sys.argv[idx:]:
        if not fn.endswith('.json'):
            fn = fn + '.json'
        md = read_meta(fn, workflows)
        typ = md['type']
        wf = workflows[typ]
#        if typ != "Metagenome":
#            print("TODO: Add support for non-Metagenome workflows (%s)" % (fn))
#            continue
        submit(conf, md, wf, dryrun=dryrun, verbose=verbose, force=force)
         
