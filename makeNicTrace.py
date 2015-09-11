#!/usr/bin/python

import sys
import random

# import pdb


def readFlows(fname):
    with open(fname) as f:
        return map(lambda l: {'time': float(l[0]), 'src': l[1].split('.')[0], 'dst': l[2][:-1].split('.')[0], 'size': int(l[3])}, (l.split() for l in f.readlines()))


def readFiles(fnames):
    fns = {}
    for fname in fnames:
        node = fname.split('/')[-1].split('-')[0]
        # pdb.set_trace()
        if ('-nic-' in fname):
            fns[node] = fname
        elif 'addr_mapping.txt' == fname:
            fns['nicmap'] = fname
        else:
            continue

    return {node: readFlows(fns[node]) for node in fns.keys()}, fns['nicmap']


def readNicMap(fname):
    with open(fname) as f:
        return {sp[-1]: int(sp[0]) for sp in (l.split() for l in f.readlines())}


def makeFlows(nodes, mapfn):
    mapping = readNicMap(mapfn)
    random.seed(0)
    # hosts = random.sample(xrange(144), len(nodes))

    earliestTime = min(f['time'] for f in sum(nodes.values(), []))
    return sorted(sum(([{
                      'time': f['time'] - earliestTime,
                      'src': int(mapping[f['src']]),
                      'dst': int(mapping[f['dst']]),
                      'size': f['size']
                      } for f in nodes[n]]
                  for n in nodes), []), key=lambda f: f['time'])


def run(outDir, traces):
    outfname = outDir + 'nic_flows.txt'
    nodes, nicmapfn = readFiles(traces)
    flows = makeFlows(nodes, nicmapfn)
    fid = 0
    with open(outfname, 'w') as of:
        for f in flows:
            of.write("{0} {1} {2} {3} {4} nic 0-0\n".format(fid, f['time'], f['src'], f['dst'], f['size']))
            fid += 1


if __name__ == '__main__':
    if (len(sys.argv) < 2):
        print 'Usage: python makeFlowTrace.py <outfile> <IO traces...>'
        sys.exit(1)
    run(sys.argv[1], sys.argv[2:])
