import sys
import os
import json
from common import init_nmdc_mongo
from common import get_activity_from_json
from common import read_list


def get_kegg_counts(fn, id):
    # Yes: We could do a json load but that can be slow for these large
    # files.  So let's just grab what we need
    cts = {}
    fct = 0
    with open(fn) as f:
        for line in f:
           fct += 1
           if 'KEGG.ORTHOLOGY' in line:
               part = line.split('.')[1][0:-2]
               func = 'KEGG.%s' % (part)
               if func not in cts:
                   cts[func] = 0
               cts[func] += 1

    rows = []
    for func, ct in cts.items():
        rec = {
               'metagenome_annotation_id' : id,
               'gene_function_id' : func,
               'count' : ct
              }
        rows.append(rec)
    print(' - %d terms, %dM annotations' % (len(rows), fct/1000000))
    return rows


if __name__ == "__main__":
    base = "/global/cfs/cdirs/m3408/ficus/pipeline_products/"
    nmdc = init_nmdc_mongo()
    if sys.argv[1] == "mongo":
        acts = []
        for actrec in nmdc.metagenome_annotation_activity_set.find({}):
            # New annotations should have this
            if 'part_of' not in actrec:
                continue 
            act = actrec['part_of'][0]
            acts.append(act)
    else:
        acts = read_list(sys.argv[1])

    for act in acts:
        actj = get_activity_from_json("annotation", act)
        if not actj:
            print("Missing: %s" % (fn))
            continue
        id = actj['id']
        fn = os.path.join(base, act, "annotation/annotations.json")
        ct = nmdc.functional_annotation_agg.count_documents({"metagenome_annotation_id": id})
        if ct > 0:
            print("Done: %s %s (%d)" % (act, id, ct))
            continue
        print("Processing %s" % (act))
        fn = os.path.join(base, act, "annotation", 'annotations.json')
        print(" - scan %s" % (fn))
        rows = get_kegg_counts(fn, id)

        print(' - %s' % (str(rows[0])))
        print(" - load %s" % (fn))
        nmdc.functional_annotation_agg.insert_many(rows)

# Schema
#
#       metagenome_annotation_id        |   gene_function_id    | count 
#---------------------------------------+-----------------------+-------
# nmdc:006424afe19af3c36c50e2b2e68b9510 | KEGG.ORTHOLOGY:K00001 |   145
