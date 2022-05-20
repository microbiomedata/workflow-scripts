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
from nmdcapi import nmdcapi
from jamo import Jamo

class watcher():
    conf = config()
    jamo = Jamo()
    dst_path = conf.conf['raw_dir']
    watch_list_file = conf.conf['watch_list']
    nmdc = nmdcapi()

    def refresh_list(self):
        wl = []
        with open(self.watch_list_file) as f:
            for item in f:
                wl.append(item.rstrip()) 
        self.proposal_list = wl

    def watch(self):
        self.cycle()

    def is_registered(self, fn):
        rf = os.path.join(os.path.dirname(fn),
                          '.' + os.path.basename(fn) + '.reg')
        return os.path.exists(rf)


    def register(self, fn):
        workflows = self.conf.workflows
        rf = os.path.join(os.path.dirname(fn),
                          '.' + os.path.basename(fn) + '.reg')
        if os.path.exists(rf):
            # Already registered
            print("Previously registered")
            return

        jsonf = fn + '.json'
        md = json.loads(open(jsonf).read())

        # Extract parameters
        product = md['metadata']['sequencing_project']['sequencing_product_name']
        typ = workflows["Product Mappings"][product]
        if typ not in workflows["Object Type Mappings"]:
            print("%s not implemented yet" % (typ))
            return
        otype = workflows["Object Type Mappings"][typ]
        name = os.path.split(fn)[-1]
        # We are using the description field to encode information for now.
        desc = {
                  "proj": 'gold:' + md['metadata']['gold_data']['gold_stamp_id'],
                  "prod": product,
                  "type": typ
                }
        desc_enc = json.dumps(desc)
        data_url = '%sraw/%s' % (self.conf.conf['url_root'], name)
        # Move to config
        print("registering %s" % (fn))
        self.nmdc.refresh_token()
        obj = self.nmdc.create_object(fn, desc_enc, data_url)
        if 'detail' in obj:
            print(obj)
            return
        oid = obj['id']
        self.nmdc.set_type(oid, otype)
        with open(rf, 'w') as f:
            f.write(json.dumps(obj))
        print("registered %s as %s" % (fn, oid))
        #sys.exit(1)

    def cycle(self):
        req_list = []
        self.refresh_list()
        for prop in self.proposal_list:
            data = self.jamo.get_list(int(prop))

            for rec in data['records']:
                id = rec['_id']
                state = rec['file_status']
                fname = rec['file_name']
                fsize = rec['file_size']
                dst = os.path.join(self.dst_path, fname)
                if self.is_registered(dst):
                    continue
                if os.path.exists(dst):
                    st = os.stat(dst)
                    if st.st_size == fsize:
                        self.register(dst)
                        continue
                if state == "PURGED":
                    print("request %s" % (rec['_id']))
                    req_list.append(id)
                elif state == "RESTORED":
                    src = os.path.join(rec['file_path'], fname)
                    self.jamo.copy(src, dst, rec, overwrite=True)

        resp = self.jamo.fetch(req_list)
    


if __name__ == "__main__":
    w = watcher()
    if sys.argv[1] == 'watch':
        w.watch()
