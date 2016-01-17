#!/usr/bin/python3

import sys
import math
import itertools
import random
import subprocess

import pdb

#
# Create interarrival and size CDFs for each src, dst pair.
#

def readFlows(filename):
    with open(filename, 'r') as f:
        for line in f.readlines():
            elem = line.split()
            yield {
                    'src': int(elem[2]),   #1
                    'dst': int(elem[3]),   #2
                    'time': float(elem[1]),#3
                    'size': int(elem[4])   #4
                    }

def groupSD(flows):
    return itertools.groupby(sorted(flows, key=lambda f: (f['src'], f['dst'])), lambda f: (f['src'], f['dst']))

def getInterarrivals(flows):
    #flows are already sorted in time order
    t = 0
    for f in flows:
        yield f['time'] - t
        t = f['time']

def getSizes(flows):
    return map(lambda f: f['size'], flows)

# takes list of numbers (not iterator)
# returns two lists, the same list (xaxis) and cdf value (yaxis)
def getCdf(nums):
    length = len(nums)
    # cdf with at most 65536 lines
    if (length >= 65536):
        nums = random.sample(nums, 65533)
        nums.append(min(nums))
        nums.append(max(nums))
        length = 65535
    nums.sort()

    ns = []
    for k, g in itertools.groupby(nums):
        ns.append((k, sum(1 for _ in g)))

    def cumsum(n):
        tot = 0
        for i in n:
            tot += i
            yield float(tot) / len(nums)
    yaxis = list(cumsum(map(lambda t: t[1], ns)))

    assert(len(yaxis) == len(ns))
    if (yaxis[-1] != 1.0):
        pdb.set_trace()
    assert yaxis[-1] == 1.0, yaxis[-1]
    return (ns, yaxis)

def writeCdf(filename, cdftup):
    with open(filename, 'w') as f:
        for x, y in zip(*cdftup):
            f.write("{} {} {}\n".format(x[0], x[1], y))

fns = sys.argv[1:]
for fn in fns:
    print(fn)

    cname = '/'.join(fn.split('/')[:-2]) + '/cdf/'
    subprocess.call(['mkdir', '-p', cname])

    for sd, sdFlowGroup in groupSD(readFlows(fn)):
        flows = list(sdFlowGroup)
        print(sd, 'num flows in group', len(flows))

        if (len(flows) < 3):
            continue

        inters = list(getInterarrivals(flows))
        sizes = list(map(lambda i:math.ceil(i/1460), getSizes(flows)))

        interCdf = getCdf(inters)
        sizesCdf = getCdf(sizes)

        writeCdf(cname + '{}_{}_sizes.cdf'.format(sd[0], sd[1]), sizesCdf)
        writeCdf(cname + '{}_{}_interarrivals.cdf'.format(sd[0], sd[1]), interCdf)