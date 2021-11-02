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
from wfutils import config 


class Jamo():
    _base_url = "https://sdm2.jgi-psf.org/api/"
    _types = ["Metagenome Standard Draft",
              "Metagenome Minimal Draft",
              "Metagenome Metatranscriptome"]

    def get_list2(self, key, val):
        url = self._base_url + "metadata/pagequery"
        q = {
             key: val,
             "metadata.rqc.usable": True,
             "metadata.sequencing_project.sequencing_product_name": {
                 "$in": self._types
                 }
            }
        print(q)
        print(url)
        resp = requests.post(url, data=json.dumps({"query": q}))
        print(resp)
        return resp.json()

    def get_list(self, prop):
        url = self._base_url + "metadata/pagequery"
        q = {
             "metadata.proposal_id": prop,
             "metadata.rqc.usable": True,
             "metadata.sequencing_project.sequencing_product_name": {
                 "$in": self._types
                 }
            }

        resp = requests.post(url, data=json.dumps({"query": q}))
        return resp.json()

    def fetch(self, ids):
        url = self._base_url + "tape/grouprestore"

        resp = requests.post(url, data=json.dumps({"files": ids, "requestor": "canon"}))
        return resp.json()

    def copy(self, src, dst, rec=None, overwrite=False):
        if not os.path.exists(dst) or overwrite:
            print("Copy: %s %s" % (src, dst))
            shutil.copyfile(src, dst)
            if rec:
                with open(dst + ".json", "w") as f:
                     f.write(json.dumps(rec, indent=2))

def jprint(obj):
    print(json.dumps(obj, indent=2))

if __name__ == "__main__":
    conf = config()
    do_fetch = False
    jamo = Jamo()
    if len(sys.argv) == 1:
        print("usage: jamo (fetch|list) <proposal> [path]")
        sys.exit(1)
    comm = sys.argv[1]
    dst_path = conf.get_stage_dir()
    if comm == "fetch":
        do_fetch = True
        prop = sys.argv[2]
        if len(sys.argv) > 3:
            dst_path = sys.argv[3]
    elif comm == "list":
        prop = sys.argv[2]
        resp = jamo.get_list(int(prop))
        jprint(resp)
        sys.exit()
    elif comm == "list2":
        key = sys.argv[2]
        val = sys.argv[3]
        resp = jamo.get_list2(key, val)
        print(resp)
        sys.exit()
    else:
        print("usage")
        sys.exit(1)

    data = jamo.get_list(int(prop))

    req_list = []
    for rec in data['records']:
        id = rec['_id']
        state = rec['file_status']
        fname = rec['file_name']
        fsize = rec['file_size']
        gs = rec['metadata']['gold_data']['gold_stamp_id']
        print(id, fname, fsize/(1024*1024*1024), rec['metadata']['sequencing_project']['sequencing_product_name'], gs, state)
        if do_fetch:
            if state == "PURGED":
                print("request %s" % (rec['_id']))
                req_list.append(id)
            elif state == "RESTORED" and do_fetch:
                src = os.path.join(rec['file_path'], fname)
                dst = os.path.join(dst_path, fname)
                jamo.copy(src, dst, rec)

    resp = jamo.fetch(req_list)


