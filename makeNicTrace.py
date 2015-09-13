#!/usr/bin/python

import sys
import random

# import pdb


def readFlows(fname):
    def stripPort(name):
        return '.'.join(name.split('.')[:-1])

    with open(fname) as f:
        return map(lambda l: {'time': float(l[0]), 'src': stripPort(l[2]), 'dst': stripPort(l[3]), 'size': int(l[4])}, (l.split() for l in f.readlines()))


def readFiles(fnames):
    fns = {}
    for fname in fnames:
        if 'addr_mapping.txt' in fname:
            fns['nicmap'] = fname
            continue
        node = fname.split('/')[-1].split('-')[0]
        # pdb.set_trace()
        if ('-nic-' in fname):
            fns[node] = fname
        else:
            continue

    return {node: readFlows(fns[node]) for node in fns.keys() if node != 'nicmap'}, fns['nicmap']


def readNicMap(fname):
    with open(fname) as f:
        return {sp[-1]: int(sp[0]) for sp in (l.split() for l in f.readlines())}


def makeFlows(nodes, mapfn):
    mapping = readNicMap(mapfn)
    print mapping
    random.seed(0)
    # hosts = random.sample(xrange(144), len(nodes))

    earliestTime = min(f['time'] for f in sum(nodes.values(), []))
    return sorted(sum(([{
                      'time': f['time'] - earliestTime,
                      'src': int(mapping[f['src']]),
                      'dst': int(mapping[f['dst']]),
                      'size': f['size']
                      } for f in nodes[n] if (f['src'] in mapping and f['dst'] in mapping)]
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
