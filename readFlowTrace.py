#!/usr/bin/python

import sys

import numpy as np

def readFlows(filename):
    return [
            {
                'id':int(sp[0]),
                'time':float(sp[1]),
                'src':int(sp[2]),
                'dst':int(sp[3]),
                'size':float(sp[4]),
                'type':sp[5]
            } for sp
        in [l.split() for l in open(filename)]
    ]

def outputSimulatorFriendly(flows):
    flows.sort(key = lambda x: x['time'])
    template = "{} {} 0 0 {} 0 0 {} {}"
    for f in flows:
        #print f['size'], f['size']/1460, np.ceil(f['size']/1460)
        print template.format(f['id'], f['time']/1e6 + 1.0, int(np.ceil(f['size']/1460)), f['src'], f['dst'])

def sliceByTime(flows):
    flows.sort(key = lambda x: x['time'])
    times = range(1,296)
    flowsByTime = []
    sliceStart = 0
    for time in times:
        cutoff = time * 1e6
        flowsAtTime = [f for f in flows if f['time'] >= sliceStart and f['time'] < cutoff]
        flowsByTime.append(flowsAtTime)
        sliceStart = cutoff
    return flowsByTime

if __name__ == '__main__':
    if (len(sys.argv) < 2):
        print 'Usage: python readFlowTrace.py <flows.txt>'
        sys.exit(1)
    flows = readFlows(sys.argv[1])
    lengths = [f['size'] for f in flows]
    hist = np.histogram(lengths)
    #print 'Histogram'
    #print hist

    byTime = sliceByTime(flows)
    #print 'by time'
    #print map(len, byTime)

    outputSimulatorFriendly(flows)
