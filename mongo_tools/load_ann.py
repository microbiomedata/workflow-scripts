import sys
import os
import json
from common import init_nmdc_mongo
from common import get_activity_from_json
from common import read_list
from time import time


base = "/global/cfs/cdirs/m3408/ficus/pipeline_products/"


def process_annotations(act, id):
    fn = os.path.join(base, act, "annotation", "annotations.json")
    print("- loading %s" % (fn))
    d = json.load(open(fn))
    rows = d['functional_annotation_set']
    nrows = []
    for r in rows:
        if not r['has_function'].startswith('KEGG.ORTHOLOGY'):
            continue
        r['subject'] = r['subject'].replace('nmdc:nmdc:', 'nmdc:')
        r['was_generated_by'] = id
        nrows.append(r)
    return nrows

def process_features(act):
    fn = os.path.join(base, act, "annotation", "features.json")
    print("- loading %s" % (fn))
    d = json.load(open(fn))
    rows = d['genome_feature_set']
    for r in rows:
        r['encodes'] = r['encodes'].replace('nmdc:nmdc:', 'nmdc:')
        r['seqid'] = r['seqid'].replace('nmdc:nmdc:', 'nmdc:')
    return rows

if __name__ == "__main__":
    nmdc = init_nmdc_mongo()
    actids = read_list(sys.argv[1])
    loaded = nmdc.functional_annotation_set.distinct("was_generated_by")
    for act in actids:
        print("Processing %s" % (act))
        actj = get_activity_from_json("annotation", act)
        if not actj:
            print("Missing: %s" % (act))
            continue
        start = time()
        id = actj['id']
        gid = actj['was_informed_by'] 
        if id in loaded:
            print("Done: %s %s" % (act, gid))
            continue
        rows = process_annotations(act, id)
        if len(rows) > 0:
            nmdc.functional_annotation_set.insert_many(rows)
            rows = process_features(act)
            print("- Loading Mongo")
            nmdc.genome_feature_set.insert_many(rows)
            print("- Done (%d s)" % (time()-start))
