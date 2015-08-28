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
        else:
            continue

    return {node: readFlows(fns[node]) for node in fns.keys()}


def mapNicHostnameToNodes(nodes):
    def strip_port(hostname):
        return hostname.split('.')[0]

    stringToNodeMap = {}
    for n in nodes.keys():
        intersect = reduce(lambda s1, s2: s1 & s2, (set(map(strip_port, (f['src'], f['dst']))) for f in nodes[n]))
#        pdb.set_trace()
        if (len(intersect) != 1):
            print intersect
            assert(False)
        stringToNodeMap[intersect.pop()] = n
    return stringToNodeMap


def makeFlows(nodes):
    # mapping = mapNicHostnameToNodes(nodes)
    random.seed(0)
    # hosts = random.sample(xrange(144), len(nodes))

    earliestTime = min(f['time'] for f in sum(nodes.values(), []))
    return sorted(sum(([{
                      'time': f['time'] - earliestTime,
                      'src': f['src'],  # hosts[int(mapping[f['src']])],
                      'dst': f['dst'],  # hosts[int(mapping[f['dst']])],
                      'size': f['size']
                      } for f in nodes[n]]
                  for n in nodes), []), key=lambda f: f['time'])

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
            of.write("{0} {1} {2} {3} {4} nic 0-0\n".format(fid, "%d" % f['time'], f['src'], f['dst'], f['size']))
            fid += 1
