#!/usr/bin/env python

import json
import sys
import os 
import requests
from hashlib import sha256
import hashlib
import mimetypes
from time import time
from datetime import datetime


def _get_sha256(fn): 
    hashfn = fn + '.sha256'
    if os.path.exists(hashfn):
        with open(hashfn) as f:
            sha = f.read().rstrip()
    else:
        print("hashing %s" % (fn))
        shahash = hashlib.sha256()
        with open(fn,"rb") as f:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: f.read(1048576),b""):
                shahash.update(byte_block)
        sha = shahash.hexdigest()
        with open(hashfn, 'w') as f:
            f.write(sha)
            f.write('\n')
    return sha

class remote_job():
    def __init__(self, jid, jdata):
        self.jobdata = jdata
        self.id = jid
        self.claimed = None
        self.opid = None

    def claim(self):
        self.claimed = True


class nmdcapi():
    _base_url = 'https://api.dev.microbiomedata.org/'

    def __init__(self):
        self.get_token()

    def get_token(self):
        """
        Get a token using a client id/secret.
        """
        cfile = os.path.join(os.environ['HOME'], '.nmdc-creds.json')
        if not os.path.exists(cfile):
            self.token = None
            return

        with open(cfile) as f:
            creds = json.loads(f.read())
        h = {
                'accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
        data = {
                'grant_type': 'client_credentials',
                'client_id': creds['client_id'],
                'client_secret': creds['client_secret'],
                }
        url = self._base_url + 'token'
        resp = requests.post(url, headers=h, data=data).json()
        expt = resp['expires']
        self.expires = time() + expt['minutes'] * 60

        self.token = resp['access_token']
        self.header = {
                       'accept': 'application/json',
                       'Content-Type': 'application/json',
                       'Authorization': 'Bearer %s' % (self.token)
                       }
        return resp

    def get_header(self):
        return h

    def mint(self, ns, typ, ct):
        """
        Mint a new ID.
        Inputs: token (obtained using get_token)
                namespace (e.g. nmdc)
                type/shoulder (e.g. mga0, mta0)
                count/number of IDs to generate
        """
        url = self._base_url + 'ids/mint'
        d = { 
                "populator": "",
                "naa": ns,
                "shoulder": typ,
                "number": ct
            }
        resp = requests.post(url, headers=self.header, data=json.dumps(d))
        return resp.json()

    def get_object(self, obj, decode=False):
        """
        Helper function to get object info
        """
        url = '%sobjects/%s' % (self._base_url, obj)
        
        resp = requests.get(url, headers=self.header)
        data = resp.json()
        if decode and 'description' in data:
            data['metadata'] = json.loads(data['description'])

        return data


    def create_object(self, fn, description, dataurl):
        """
        Helper function to create an object.
        """
        url = self._base_url + 'objects'
        
        fmeta = os.stat(fn)
        name = os.path.split(fn)[-1]
        mt = mimetypes.MimeTypes().guess_type(fn)[1]
        sha = _get_sha256(fn)
        now = datetime.today().isoformat()
        d = {
             "aliases": None,
             "description": description,
             "mime_type": 'application/%s' % (mt),
             "name": name,
             "access_methods": [
               {
                 "access_id": None,
                 "access_url": {
               "url": dataurl,
                 },
                 "region": None,
                 "type": "https"
               }
             ],
             "checksums": [
               {
                 "checksum": sha,
                 "type": "sha256"
               }
             ],
             "contents": None,
             "created_time": now,
             "size": fmeta.st_size,
             "updated_time": None,
             "version": None,
             "id": sha,
             "self_uri": "todo"
       }
        resp = requests.post(url, headers=self.header, data=json.dumps(d))
        return resp.json()

    def set_type(self, obj, typ):
        url = '%sobjects/%s/types' % (self._base_url, obj)
        
        d = [typ]
        resp = requests.put(url, headers=self.header, data=json.dumps(d))
        return resp.json()

    def bump_time(self, obj):
        url = '%sobjects/%s' % (self._base_url, obj)
        now = datetime.today().isoformat()

        d = {
             "created_time": now
             }
        resp = requests.patch(url, headers=self.header, data=json.dumps(d))
        return resp.json()

    def list_jobs(self, filt=None):
        url = '%sjobs?max_page_size=200' % (self._base_url)
        d = {}
        if filt:
            url += '&filter=%s' % (json.dumps(filt))
        resp = requests.get(url, headers=self.header)
        data = resp.json()
        if 'resources' in data:
            return data['resources']
        else:
            return []

    def get_job(self, job):
        url = '%sjobs/%s' % (self._base_url, job)
        resp = requests.get(url, headers=self.header)
        return resp.json()

    def claim_job(self, job):
        url = '%sjobs/%s:claim' % (self._base_url, job)
        resp = requests.post(url, headers=self.header)
        if resp.status_code == 409:
            claimed = True
        else:
            claimed = False
        data = resp.json()
        data['claimed'] = claimed

        return data

    def list_ops(self, filt=None):
        url = '%soperations' % (self._base_url)
        d = {}
        if filter:
            d['filter'] = json.dumps(filt)
        # TODO: Handle pagination
        d['max_page_size'] = 200

        resp = requests.get(url, data=json.dumps(d), headers=self.header)
        return resp.json()['resources']

    def get_op(self, opid):
        url = '%soperations/%s' % (self._base_url, opid)
        resp = requests.get(url, headers=self.header)
        return resp.json()

    def update_op(self, opid, done=None, results=None, meta=None):
        url = '%soperations/%s' % (self._base_url, opid)
        d = dict()
        if done is not None:
            d['done'] = done
        if results:
            d['result'] = results
        if meta:
            d['metadata'] = meta
        resp = requests.patch(url, headers=self.header, data=json.dumps(d))
        return resp.json()


def usage():
    print("usage: ....")

if __name__ == "__main__":
    nmdc = nmdcapi()
    if len(sys.argv) < 2:
        usage()
    elif sys.argv[1] == 'set_type':
        obj = sys.argv[2]
        typ = sys.argv[3]
        nmdc.set_type(obj, typ)
    elif sys.argv[1] == 'get_job':
        obj = sys.argv[2]
        print(nmdc.get_job(obj))
    elif sys.argv[1] == 'bump_time':
        obj = sys.argv[2]
        nmdc.bump_time(obj)
    elif sys.argv[1] == 'get_op':
        opid = sys.argv[2]
        print(json.dumps(nmdc.get_op(opid), indent=2))
    else:
        usage()




