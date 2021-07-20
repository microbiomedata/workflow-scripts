#!/usr/bin/env python
"""
This script will query jamo based on a proposal ID and can optionally
request the files be fetched.

It will also do a copy but for this to work the user must be a member of
the NERSC genome file group.



"""
import requests
import json
import sys
import os
import shutil


def get_list(base_url, prop):
    url = base_url + "metadata/pagequery"
    types = ["Metagenome Standard Draft",
             "Metagenome Minimal Draft",
             "Metagenome Metatranscriptome"]
    q = {
         "metadata.proposal_id": int(prop),
         "metadata.rqc.usable": True,
         "metadata.sequencing_project.sequencing_product_name": {"$in": types}
        }



    resp = requests.post(url, data=json.dumps({"query": q}))
    return resp.json()

def fetch(base_url, ids):
    url = base_url + "tape/grouprestore"

    resp = requests.post(url, data=json.dumps({"files": ids, "requestor": "canon"}))
    return resp.json()

def copy(src, dst):
    if not os.path.exists(dst):
        print("Copy: %s %s" % (src, dst))
        shutil.copyfile(src, dst)



if __name__ == "__main__":
    url = "https://sdm2.jgi-psf.org/api/"

    do_fetch = False

    if sys.argv[1] == "-f":
        do_fetch = True
        prop = sys.argv[2]
        idx = 2
    else:
        prop = sys.argv[1]
        idx = 1
    if len(sys.argv) > idx+1:
       dst_path = sys.argv[idx+1]
    else:
       dst_path = None
    data = get_list(url, prop)


    req_list = []
    for row in data['records']:
        id = row['_id']
        state = row['file_status']
        fname = row['file_name']
        fsize = row['file_size']
        gs = row['metadata']['gold_data']['gold_stamp_id']
        print(id, fname, fsize/(1024*1024*1024), row['metadata']['sequencing_project']['sequencing_product_name'], gs, state)
        if state == "PURGED" and do_fetch:
            print("request %s" % (row['_id']))
            req_list.append(id)
        elif state == "RESTORED" and dst_path:
            src = os.path.join(row['file_path'], fname)
            dst = os.path.join(dst_path, fname)
            copy(src, dst)
            with open(dst + ".json", "w") as f:
                 f.write(json.dumps(row, indent=2))

    resp = fetch(url, req_list)
    print(resp)

