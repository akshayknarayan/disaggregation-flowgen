#!/usr/bin/python

import sys
import itertools
import random
import numpy as np

def readFlows(fname):
    with open(fname) as f:
        return map(lambda l:{'time':l[0], 'src':l[1], 'dst':l[2], 'size':l[3]}, (l.split() for l in f.readlines()))

def readFiles(fnames):
    fns = {}
    for fname in fnames:
        node = fname.split('-')[0]
        if (node not in fns):
            fns[node] = {}
        elif ('-nic-' in fname):
            fns[node]['nic'] = fname
        elif ('-mem-' in fname or '-disk-' in fname or '-meta-' in fname):
            continue
        else:
            assert(False)

    return {node:readFlows(fns[node]['nic']) for node in fns.keys()}

def mapNicHostnameToNodes(nodes):
    stringToNodeMap = {}
    for n in nodes.keys():
        intersect = reduce(lambda s1, s2: s1 & s2, (set((f['src'], f['dst'])) for f in nodes[n]))
        if (len(intersect) != 1):
            assert(False)
        stringToNodeMap[intersect.pop()] = n
    return stringToNodeMap

def makeFlows(nodes):
    mapping = mapNicHostnameToNodes(nodes)
    random.seed(0)
    hosts = random.sample(xrange(144), len(nodes))

    earliestTime = min(f['time'] for f in sum(nodes.values(), []))
    return sum((
        [{
        'time':f['time'] - earliestTime,
        'src':hosts[int(mapping[f['src']])],
        'dst':hosts[int(mapping[f['dst']])],
        'size':f['size']
        } for f in nodes[n]]
    for n in nodes), [])

if __name__ == '__main__':
    if (len(sys.argv) < 2):
        print 'Usage: python makeFlowTrace.py <outfile> <IO traces...>'
        sys.exit(1)
    outfname = sys.argv[1]
    nodes = readFiles(sys.argv[2:])
    flows = makeFlows(nodes)
    fid = 0
    with open(outfname, 'w') as of:
        for f in flows:
            of.write("{0} {1} {2} {3} {4}\n".format(fid, "%.9f" % f['time'], f['src'], f['dst'], f['size']))
            fid += 1
