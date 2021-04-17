#!/usr/bin/env python

import requests
import json
import sys
import os
import tempfile

def read_config():
    """
    Read config file for URL, WDL dir and template dir
    """
    cf = os.path.join(os.environ['HOME'], '.wf_config')
    conf = dict()
    with open(cf) as f:
        for line in f:
            if line.startswith("#"):
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

# Parameters
#_label_tmpl = "/global/cfs/cdirs/m3408/aim2/tools/labels.json.tmpl"

#_url = "http://cori21-ib0:8088/api/workflows/v1"



def submit(url, fna, pid, wdl, tmpl_dir, wdl_dir, bundle_fn='bundle.zip', options=None):
    """
    Submit a job
    """
    label_tmpl = os.path.join(tmpl_dir, 'labels.json.tmpl')

    inputs = {
      "annotation.imgap_input_fasta": fna,
      "annotation.database_location": "/refdata/img/",
      "annotation.imgap_project_id": pid
    }

    # Write input file
    infp, infname = tempfile.mkstemp(suffix='.json')
    with os.fdopen(infp, 'w') as fd:
        fd.write(json.dumps(inputs))

    with open(label_tmpl) as f:
        labels = json.loads(f.read())

    labels["project_id"] = pid

    lblp, lblname = tempfile.mkstemp(suffix='.json')
    with os.fdopen(lblp, 'w') as fd:
        fd.write(json.dumps(inputs))

    if not wdl.startswith('/'):
        wdl = os.path.join(wdl_dir, wdl)
    bundle = os.path.join(wdl_dir, bundle_fn)
    files = {
        'workflowSource': open(wdl),
        'workflowInputs': open(infname),
        'labels': open(lblname),
        'workflowDependencies': open(bundle, 'rb')
    }
    if options:
        files['workflowOptions']=open(options)

#    resp = requests.post(url, data={}, files=files, verify=False)
    for fld in files:
        files[fld].close()
    os.unlink(infname)
    os.unlink(lblname)

#    print(str(resp.text))
    job_id = json.loads(resp.text)['id']
    return job_id

def log_submit(job_id):
    with open('last_submit', 'w') as f:
        f.write(job_id)

if __name__ == '__main__':
    conf = read_config()
    if len(sys.argv) < 4:
        print("Usage: %s <fna> <p_id> <wdl>" % (sys.argv[0]))
        sys.exit(1)
    fna = sys.argv[1]
    pid =  sys.argv[2]
    wdl =  sys.argv[3]
    if not os.path.exists(fna):
       print("Didn't find fasta file {}".format(fna))
       sys.exit(1)

    url = conf['url'] 
    job_id = submit(conf['url'], fna, pid, wdl, conf['template_dir'], conf['wdl_dir'])
    log_submit(job_id)
