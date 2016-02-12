#!/usr/bin/python3

import sys
import math
import itertools
import random
import subprocess
import threading

import pdb

#
# Create interarrival and size CDFs for each src, dst pair.
#

def readFlows(flows):
    with open(flows, 'r') as f:
        for line in f.readlines():
            elem = line.split()
            yield {
                    'src': int(elem[2]),   #1
                    'dst': int(elem[3]),   #2
                    'time': float(elem[1]),#3
                    'size': int(elem[4]),   #4
                    'type': elem[5]
                  }

def groupSD(flows):
    return itertools.groupby(sorted(flows, key=lambda f: (f['src'], f['dst'])), lambda f: (f['src'], f['dst']))

# flows between one sd pair
def squashCoArrivals(flows):
    flows.sort(key=lambda f:f['time'])
    prev = flows[0]
    for f in flows:
        if (f['time'] - prev['time'] > 0):
            yield prev
            prev = f
        else:
            prev['size'] += f['size']

#flows are already sorted in time order
def getInterarrivals(flows):
    t = 0
    for f in flows:
        yield f['time'] - t
        t = f['time']

def getSizes(flows):
    for f in flows:
        if 'mem' in f['type']:
            yield f['size']
        else:
            yield f['size']/8  # /8 because blkparse outputs in 512-byte *sectors*, not 4096-byte *blocks* :'(

# takes list of numbers (not iterator)
# returns two lists, the same list (xaxis) and cdf value (yaxis)
# fixed: get cdf, then sample.
def getCdf(nums):
    nums.sort()
    ns = []
    for k, g in itertools.groupby(nums):
        ns.append((k, sum(1 for _ in g)))
    def cumsum(n):
        tot = 0
        for i in n:
            tot += i
            yield float(tot) / len(nums)
    yaxis = list(cumsum(t[1] for t in ns))

    if (len(ns) >= 65536):
        length = len(ns)
        idxs = random.sample([i for i in range(1, length-1)], 65533)
        idxs.append(0)
        idxs.append(length-1)
        idxs.sort()
        sampled_nums = [ns[i] for i in idxs]
        sampled_cdf  = [yaxis[i] for i in idxs]
    else:
        sampled_nums = ns
        sampled_cdf = yaxis
    assert(len(sampled_nums) == len(sampled_cdf))
    assert sampled_cdf[-1] == 1.0, sampled_cdf[-1]
    return (sampled_nums, sampled_cdf)

def writeCdf(filename, cdftup):
    with open(filename, 'w') as f:
        for x, y in zip(*cdftup):
            f.write("{} {} {}\n".format(x[0], x[1], y))

flowfn = sys.argv[1]
print(flowfn)

cname = '/'.join(flowfn.split('/')[:-2]) + '/cdf/'
subprocess.call(['mkdir', '-p', cname])

def processSD(sd, flows):
    print(sd, 'num flows in group', len(flows))

    if (len(flows) < 5):
        return

    flows = list(squashCoArrivals(flows))

    inters = list(getInterarrivals(flows))
    sizes = list(map(lambda i:math.ceil(i/1460), getSizes(flows)))

    interCdf = getCdf(inters)
    sizesCdf = getCdf(sizes)

    writeCdf(cname + '{}_{}_sizes.cdf'.format(sd[0], sd[1]), sizesCdf)
    writeCdf(cname + '{}_{}_interarrivals.cdf'.format(sd[0], sd[1]), interCdf)

threads = []
for sd, sdFlowGroup in groupSD(readFlows(flowfn)):
    threads.append(threading.Thread(target=processSD, args=(sd, list(sdFlowGroup))))

[t.start() for t in threads]
[t.join() for t in threads]
