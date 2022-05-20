import os
import sys
import json
import jsonschema
from glob import glob
from common import init_nmdc_mongo


def save_data_objects(do_set):

    # Validate things
    out = {'data_object_set': do_set}

    schemafile =  os.environ.get("SCHEMA")
    schema = json.load(open(schemafile))
    jsonschema.validators.validate(out, schema)

    # Do the save.  Could use bulk?
    print(len(do_set))
    for rec in do_set:
        nmdc.data_object_set.insert_one(rec)


def scan_raw():
    sys.stderr.write("Scanning raw data json\n")
    base = "/global/cfs/cdirs/m3408/ficus/pipeline_products/"
    patt = base + 'raw/*[ATGC][ATGC].fastq.gz.json'

    fns = {}
    for f in glob(patt):
        d = json.load(open(f))
        fns['nmdc:%s' % (d['md5sum'])] = d
        fns['jgi:%s' % (d['md5sum'])] = d
        fns['jgi:%s' % (d['_id'])] = d
    return fns

def mk_rec(id, rec):
    fn = rec['file_name']
    size = rec['file_size']
    nrec = {
            "id": id,
            "name": fn,
            "description": "Raw sequencer read data",
            "file_size_bytes": size,
            "type": "nmdc:DataObject"
           }
    return nrec

if __name__ == "__main__":
    nmdc = init_nmdc_mongo()
    fns = scan_raw()
    sys.stderr.write("Getting known data objects in mongo\n")
    known = {}
    for rec in nmdc.data_object_set.find({}, {'id': True}):
        known[rec['id']] = 1

    sys.stderr.write("Scanning QC records\n")
    do_set = []
    seen = {}
    for rec in nmdc.read_QC_analysis_activity_set.find():
        wib = rec['was_informed_by']
        for oid in rec['has_input']:
            if oid in seen:
                sys.stderr.write("Duplicate reference %s in %s\n" % (oid, rec['id']))
            seen[oid] = rec 
            # Skip if it exists
            if oid in known:
                continue

            if oid not in fns:
                print("Didn't find json file for %s in %s (%s)" % (oid, rec['id'], wib))
                continue
            print("Add rec for %s" % (oid))
            nrec = mk_rec(oid, fns[oid])
            do_set.append(nrec)
    print("Need to add %d recs" % (len(do_set)))
    save_data_objects(do_set)
