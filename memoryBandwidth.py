#!/usr/bin/python

from __future__ import print_function
import sys
import math
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt

SCALE = [' ', '.', '_', '-', '~', '=', '*', '^', '!']

def readFlows(inp):
    count = 0
    for line in inp:
        sp = line.split()
        assert(sp[5] == 'memRead' or sp[5] == 'memWrite')
        yield {
            'size': int(sp[4]),
            'sd': (int(sp[1]), int(sp[2])),
            'time': float(sp[3]),
            'rw': (sp[5] == 'memRead')
        }

def bw_total(fs):
    times = [sp['time'] for sp in fs]
    elapsed = max(times) - min(times)
    totalSize = sum(sp['size'] for sp in fs)
    return totalSize / elapsed # bytes per second

def bw_time(fs, numNodes):
    fs.sort(key=lambda f:f['time'])
    slot = 0
    sizes = []
    size = 1 # to prevent log(0) problems
    for f in fs:
        if (f['time'] <= slot + 1.0): # 1 s slots
            size += f['size']
        else:
            sizes.append(size/numNodes)
            size = 1
            slot += 1.0
    #sizes_graph = [SCALE[math.floor(math.log10(s))] for s in sizes]
    return sizes #, sizes_graph

def cdf(nums):
    nums.sort()
    n = len(nums)
    yaxis = [float(i)/n for i in range(n)]
    plt.xlabel('Bytes in 1 second slot')
    plt.ylabel('CDF')
    plt.ylim(0,1)
    plt.semilogx(nums, yaxis)
    plt.savefig('mem.pdf')

fs = list(readFlows(sys.stdin))
reads = [f for f in fs if f['rw'] is True]
writes = [f for f in fs if f['rw'] is False]
print(len(fs), len(reads), len(writes))

numNodes = len(set(f['sd'][0] for f in reads))

print('Total reads: {}, {} nodes'.format(bw_total(reads)/numNodes, numNodes))
print('Total writes: {}, {} nodes'.format(bw_total(writes)/numNodes, numNodes))

# reads over time
rsz = bw_time(reads, numNodes)
cdf(rsz)
