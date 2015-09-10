#!/usr/bin/python

import sys
import itertools

import numpy as np
import scipy.stats
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt

# import pdb


def readFlows(filename):
    return [{
            'id': int(sp[0]),
            'time': float(sp[1]),
            'src': sp[2],
            'dst': sp[3],
            'size': float(sp[4]),
            'type': sp[5]
            } for sp
            in [l.split() for l in open(filename)]
            ]


def outputSimulatorFriendly(fname, flows):
    flows.sort(key=lambda x: x['time'])
    template = "{} {} 0 0 {} 0 0 {} {}\n"
    with open(fname, 'w') as out:
        for f in flows:
            out.write(template.format(f['id'], "%.9f" % (f['time'] + 1.0), int(np.ceil(f['size']/1460)), f['src'], f['dst']))


def cdf(nums, method='full'):
    if (method is 'full'):
        N = len(nums)
        xaxis = np.sort(nums)
        yaxis = np.array(range(N))/float(N)
        return xaxis, yaxis
    else:
        hist, xaxis = np.histogram(nums, normed=True)
        dx = xaxis[1] - xaxis[0]
        yaxis = np.cumsum(hist) * dx
        return xaxis[1:], yaxis


def prefixName(pref, name):
    return name if pref is None else pref + "_" + name


def flowSizes(flows, prefix=None):
    def plotSizeCDF(fs, name, fname, logx=False):
        x, y = cdf(fs)
        plt.title(name)
        plt.xlabel('Size, Bytes')
        plt.ylabel('CDF')
        plt.ylim(0, 1)
        plt.xlim(1, 10e9)
        if (logx):
            plt.semilogx(x, y)
        else:
            plt.plot(x, y)
        plt.savefig(prefixName(prefix, fname + '_cdf.png'))
        plt.clf()

    allfs = [f['size'] for f in flows]
    plotSizeCDF(allfs, 'All Flows', 'allflowsizes', logx=True)

    mems = [f['size'] for f in flows if 'mem' in f['type']]
    plotSizeCDF(mems, 'Remote Memory Flows', 'memflowsizes', logx=True)

    disk = [f['size'] for f in flows if 'disk' in f['type']]
    plotSizeCDF(disk, 'Disk Flows', 'diskflowsizes', logx=True)


def sdAnalysis(flows):
    def interarrival(flows):
        return [flows[i+1]['time'] - flows[i]['time'] for i in range(len(flows)-1)]

    hosts = set(f['src'] for f in flows) | set(f['dst'] for f in flows)
    sdpairs = sum(([(i, j) for j in hosts if i != j] for i in hosts), [])
    sdflows = {(s, d): [f for f in flows if f['src'] == s and f['dst'] == d] for s, d in sdpairs}
    sdstats = []
    for sd in sdflows.keys():
        fs = sdflows[sd]
        fs.sort(key=lambda f: f['time'])
        inter = interarrival(fs)
        stats = (len(fs), np.median(inter), min(inter), max(inter)) if len(inter) > 0 else (0, 0, 0, 0)
        sdstats.append((sd, stats))
    sdstats.sort(key=lambda x: x[0][1])
    sdstats.sort(key=lambda x: x[0][0])
    print '(src, dst)', '(num flows, median interarrival, min inter, max inter)'
    for t in sdstats:
        print t[0], t[1]


def interarrivals(times):
    old = next(times)
    for curr in times:
        yield curr - old
        old = curr


def sourceInterarrival(flows, prefix=None):
    flows.sort(key=lambda f: f['time'])
    srcs = set(f['src'] for f in flows)
    plt.cla()
    plt.clf()
    plt.title('CDF of Interarrival Times')
    plt.xlabel('Interarrival Time (us)')
    plt.ylabel('CDF')
    plt.ylim(0, 1)
    plt.xlim(0.1, 1e9)
    for s in srcs:
        inters = list(interarrivals(f['time'] * 1e6 for f in flows if f['src'] == s))
        x, y = cdf(inters)
        plt.semilogx(x, y, label=str(s))
        cmp_x = np.logspace(-1, 10)
        plt.semilogx(cmp_x, scipy.stats.expon.cdf(cmp_x, scale=np.mean(x)), '--', label='Fitted Exponential')
        print 'K-S Test', scipy.stats.kstest(x, lambda x: scipy.stats.expon.cdf(x, scale=np.mean(x)))
    plt.savefig(prefixName(prefix, 'comparefit_cdf_src_interarrivals.png'))

    plt.clf()
    plt.cla()
    plt.title('Histogram of Interarrival Times')
    plt.xlabel('Interarrival Time (us)')
    plt.ylabel('Count')
    plt.ylim(0.1, 1e3)
    for s in srcs:
        inters = list(interarrivals(f['time'] for f in flows if f['src'] == s))
        bin_vals, bin_edges, _ = plt.hist(inters, bins=np.logspace(-1, 6), log=True)
        plt.xscale('log')
        plt.xlim(0.1, 1e9)
        break
    plt.savefig(prefixName(prefix, 'pdf_src_interarrivals.png'))


def normalizedDerivative(nums):
    old = next(nums)
    for curr in nums:
        yield ((abs(curr - old))/old if (old != 0) else 0)
        old = curr


def burstinessAnalysis(flows, prefix=None):
    flows.sort(key=lambda x: x['time'])
    slotDuration = 0.1  # 100 ms= 0.1 s slots

    flowsByTime = [(k, list(f)) for k, f in itertools.groupby(flows, key=lambda i: i['time'] // slotDuration)]
    times, flowGroups = zip(*flowsByTime)
    byTime = map(lambda fs: sum(f['size'] for f in fs) * 8, flowGroups)
    xaxis = np.array(times)  # time in seconds
    yaxis = np.array(byTime) * slotDuration

    # pdb.set_trace()

    plt.clf()
    plt.cla()
    plt.title('Traffic Volume')
    plt.xlabel('Time (s)')
    plt.ylabel('bps')
    plt.ylim(1, 1e10)
    plt.semilogy(xaxis, yaxis, 'b.')
    plt.savefig(prefixName(prefix, 'trafficvolume.png'))

    plt.clf()
    plt.cla()
    plt.title('CDF of Traffic Volume')
    plt.xlabel('Traffic Volume, bps')
    plt.ylabel('CDF')
    plt.ylim(0, 1)
    plt.xlim(1, 1e10)
    x, y = cdf(yaxis)
    plt.semilogx(x, y)
    plt.savefig(prefixName(prefix, 'cdf_trafficvolume.png'))

    plt.clf()
    plt.cla()
    plt.title('Derivative of Traffic Volume')
    plt.xlabel('Time (s)')
    plt.ylabel('bp(s^2)')
    plt.ylim(1e-8, 1e8)
    tmp = list(normalizedDerivative(iter(yaxis)))
    plt.semilogy(xaxis[:-1], tmp)
    plt.savefig(prefixName(prefix, 'derivative_trafficvolume.png'))

if __name__ == '__main__':
    mode = None
    if (len(sys.argv) < 2):
        print 'Usage: python readFlowTrace.py <flows.txt>'
        sys.exit(1)
    elif (len(sys.argv) > 2):
        mode = sys.argv[1]

    flows = readFlows(sys.argv[-1])

    print 'read', len(flows), 'flows'

    flowSizes(flows, prefix=mode)
    #  sdAnalysis(flows)
    sourceInterarrival(flows, prefix=mode)
    burstinessAnalysis(flows, prefix=mode)

    #  outputSimulatorFriendly('sim_'+sys.argv[1]+'.txt', flows)
