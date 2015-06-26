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

def outputSimulatorFriendly(fname, flows):
    flows.sort(key = lambda x: x['time'])
    template = "{} {} 0 0 {} 0 0 {} {}\n"
    with open(fname, 'w') as out:
        for f in flows:
            out.write(template.format(f['id'], "%.9f" % (f['time']/1e6 + 1.0), int(np.ceil(f['size']/1460)), f['src'], f['dst']))

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

def interarrival(flows):
    return [flows[i+1]['time'] - flows[i]['time'] for i in range(len(flows)-1)]

def sdAnalysis(flows):
    hosts = set(f['src'] for f in flows)
    sdpairs = sum(([(i,j) for j in hosts if i != j] for i in hosts), [])
    sdflows = {(s,d):[f for f in flows if f['src'] == s and f['dst'] == d] for s,d in sdpairs}
    sdstats = []
    for sd in sdflows.keys():
        fs = sdflows[sd]
        fs.sort(key = lambda f:f['time'])
        inter = interarrival(fs)
        stats = (len(fs), np.median(inter), min(inter), max(inter)) if len(inter) > 0 else (0,0,0,0)
        sdstats.append((sd, stats))
    sdstats.sort(key = lambda x:x[0][1])
    sdstats.sort(key = lambda x:x[0][0])
    print '(src, dst)', '(num flows, median interarrival, min inter, max inter)'
    for t in sdstats:
        print t[0], t[1]

def burstinessAnalysis(flows):
    lengths = [f['size'] for f in flows]
    hist = np.histogram(lengths)
    print 'Histogram'
    print hist

    byTime = sliceByTime(flows)
    print 'by time'
    print map(len, byTime)

if __name__ == '__main__':
    if (len(sys.argv) < 2):
        print 'Usage: python readFlowTrace.py <flows.txt>'
        sys.exit(1)
    elif (len(sys.argv) > 2):
        mode = sys.argv[1]

    flows = readFlows(sys.argv[-1])

    sdAnalysis(flows)
#    burstinessAnalysis(flows)

    #outputSimulatorFriendly('sim_'+sys.argv[1], flows)

