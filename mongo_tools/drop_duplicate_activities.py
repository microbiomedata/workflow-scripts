from common import init_nmdc_mongo
# from common import read_list
from common import get_activity_from_json
from common import data_object_map
from common import coll
import json


"""
Find and remove activities that reference the same omic
process.  This is because we updated FICUS and SPRUCE.

"""

def dprint(str):
    return

def jprint(rec, indent=2):
    print(json.dumps(rec, indent=indent))

def read_omics(nmdc):
    # Get all omics records
    ops = {}
    for doc in nmdc.omics_processing_set.find():
        ops[doc['id']] = doc
    return ops


_GOOD = "https://github.com/microbiomedata/metaG/releases/tag/0.1"

def fix_dup(nmdc, op, c, domap):
    print("%s ======" % (op))
    kct = 0
    matches = {}
    drop = []
    for r in nmdc[c].find({'was_informed_by': op}):
        _id = r.pop('_id')
#        jprint(r)
        print(r['git_url'])
        gurl = r['git_url']
        id = r['id']
        matches[id] = r
        if gurl == _GOOD:
            kct += 1
            dprint("keep: %s %s" % (id, gurl))
        else:
            drop.append(id)
            dprint("drop: %s %s" % (id, gurl))
    if kct > 1:
        print("Still too many matches!")
        return []
    elif kct == 1:
        print("Dropping based on git url")
        return drop
   
    # Try by looking at the ids 
    kct = 0
    drop = []
    for id in matches:
        r = matches[id]
        dourl = domap[r['has_output'][0]]
        act = dourl.split('/')[-3]
        typ = dourl.split('/')[-2]
        actj = get_activity_from_json(typ, act)
        if not actj:
            print("hmmm %s %s" % (typ, act))
        actid = actj['id']
        if id == actid:
            kct += 1
            dprint("keep: %s == %s" % (id, actid))
        else:
            drop.append(id)
            dprint("drop: %s != %s" % (id, actid))
    if kct == 1:
        print("Fixed with ids")
        return drop
    print("Try part of")
    kct = 0
    drop = []
    for id in matches:
        if 'part_of' in matches[id]:
            kct += 1
            dprint("keep: %s == %s" % (id, actid))
        else:
            drop.append(id)
            dprint("drop: %s != %s" % (id, actid))
    if kct == 1:
        print("Fixed part_of")
        return drop

    print("I give up")
    return []

if __name__ == "__main__":
    nmdc = init_nmdc_mongo()
    domap = data_object_map(nmdc, verbose=True)
    # Get omics records
    ops = read_omics(nmdc)
    for op in ops:
        if op.startswith('emsl:'):
            continue
        if ops[op]['omics_type']['has_raw_value'] == 'Metatranscriptome':
            continue
        for c in coll:
            ct = nmdc[c].count_documents({'was_informed_by' : op})
            if ct > 1:
               print("Fix", op, c, ct)
               dropl = fix_dup(nmdc, op, c, domap)
               print(len(dropl))
               for id in dropl:
                   print("dropping %s" % (id))
#                   nmdc[c].delete_one({'id': id})
            if ct ==0 :
               print("Missing", op, c, ct)
