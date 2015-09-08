#!/usr/bin/python

import numpy as np
# import scipy.stats
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt

from readFlowTrace import *

import pdb

# Graph function takes 2 dicts with structure keys = application name, value = list of flows


def makeGradient_greentoBlue(steps=10):
    start = (0, 1.0, 0)
    step = 1.0 / steps
    colors = [start]
    while (start[1] > step * 3):
        g = start[1] - step
        b = start[2] + step
        start = (0, g, b)
        colors.append(start)
    colors.append((0, 0, 1.0))
    return colors


# Graph 1: Flow size distribution in \dis and \pdis. All applications go into one figure: multiple lines in the CDF, one for each application. The same thing for \pdis.
def graph1(pdis, dis):
    def makeGraph(name, apps):
        plt.cla()
        plt.clf()
        plt.ylim(0, 1)
        plt.xlim(1, 1e10)
        plt.ylabel('CDF')
        plt.xlabel('Flow Size, Bytes')
        for app in apps:
            x, y = cdf([f['size'] for f in apps[app]])
            plt.semilogx(x, y, label=app)
        plt.savefig(name)
    makeGraph('graph1_sizedist_pdis.pdf', pdis)
    makeGraph('graph1_sizedist_dis.pdf', dis)
    print 'graph1'
    return


# Graph 2: Number of flows in \dis and \pdis. One single bar graph, two bars for each application --- one for #flows in \dis and the other for #flows in \pdis.
def graph2(pdis, dis):
    keys_sorted = sorted(pdis.keys())
    pdis_numflows = [len(pdis[k]) for k in keys_sorted]
    dis_numflows = [len(dis[k]) for k in keys_sorted]
    plt.cla()
    plt.clf()
    ind = np.arange(len(pdis))
    width = 0.35
    plt.xlabel('Application')
    plt.ylabel('Number of Flows')
    plt.ylim(1e2, 1e7)
    plt.xticks(ind + width, tuple(dis.keys()))
    pdis_bar = plt.bar(ind, pdis_numflows, width, log=True, color='g')
    dis_bar = plt.bar(ind+width, dis_numflows, width, log=True, color='b')
    plt.legend((pdis_bar[0], dis_bar[0]), ('Pre-Disaggregation', 'Disaggregated'))
    plt.savefig('graph2_numflows.pdf')
    print 'graph2'
    return


# Graph 3: #bytes in different flow bucket sizes for \dis and \pdis. One single stack-bar graph, two bars for each application, one stack for each bucket size --- one for #bytes in \dis and the other for #bytes in \pdis.
def graph3(pdis, dis):
    keys_sorted = sorted(pdis.keys())
    largest_flow = max(max([max(f['size'] for f in pdis[k]) for k in keys_sorted]), max([max(f['size'] for f in dis[k]) for k in keys_sorted]))
    buckets = np.logspace(2, np.floor(np.log10(largest_flow)), num=11)
    bucketized_pdis = {k: np.histogram([f['size'] for f in pdis[k]], bins=buckets)[0] for k in keys_sorted}
    bucketized_dis = {k: np.histogram([f['size'] for f in dis[k]], bins=buckets)[0] for k in keys_sorted}

    plt.cla()
    plt.clf()
    ind = np.arange(len(pdis))
    width = 0.35
    plt.xlabel('Application')
    plt.ylabel('Number of Flows')
    plt.title('Number of Flows per Bin')
    plt.xticks(ind + width, tuple(pdis.keys()))
    bottoms_pdis = [0] * len(keys_sorted)
    bottoms_dis = [0] * len(keys_sorted)
    colors = makeGradient_greentoBlue(steps=10)
    for i in xrange(len(buckets) - 1):
        vals_pdis = [bucketized_pdis[k][i] for k in keys_sorted]
        vals_dis = [bucketized_dis[k][i] for k in keys_sorted]
        plt.bar(ind, vals_pdis, width, bottom=bottoms_pdis, log=True, color=colors[i])
        plt.bar(ind+width, vals_dis, width, bottom=bottoms_dis, log=True, color=colors[i])
        bottoms_pdis = vals_pdis
        bottoms_dis = vals_dis
    plt.savefig('graph3_bucketedFlowSizes.pdf')
    print 'graph3'
    return


# Graph 4: Flow arrival time distribution in \dis and \pdis. All the applications go into one figure: multiple lines in the CDF, one for each application. The same thing for \pdis.
def graph4(pdis, dis):
    keys_sorted = sorted(pdis.keys())

    def makeGraph(name, apps):
        plt.cla()
        plt.clf()
        plt.ylim(0, 1)
        plt.ylabel('CDF')
        plt.xlim(1e-1, 1e10)
        plt.xlabel('Interarrival Time, Microseconds')
        for app in keys_sorted:
            srcs = set(f['src'] for f in apps[app])
            inters = sum([list(interarrivals(f['time'] * 1e6 for f in sorted(filter(lambda f: f['src'] == s, apps[app]), key=lambda f: f['time']))) for s in srcs], [])
            x, y = cdf(inters)
            plt.semilogx(x, y, label=app)
        plt.savefig(name)
    makeGraph('graph4_interdist_pdis.pdf', pdis)
    makeGraph('graph4_interdist_dis.pdf', dis)
    print 'graph4'
    return


# Graph 5: Traffic volume in \dis and \pdis. One single bar graph, two bars for each application --- one for volume in \dis and the other for volume in \pdis.
def graph5(pdis, dis):
    keys_sorted = sorted(pdis.keys())
    pdis_numflows = [sum(f['size'] for f in pdis[k]) for k in keys_sorted]
    dis_numflows = [sum(f['size'] for f in dis[k]) for k in keys_sorted]
    plt.cla()
    plt.clf()
    ind = np.arange(len(pdis))
    width = 0.35
    plt.xlabel('Application')
    plt.ylabel('Traffic Volume (Bytes)')
    plt.ylim(1e5, 1e12)
    plt.xticks(ind + width, tuple(dis.keys()))
    pdis_bar = plt.bar(ind, pdis_numflows, width, color='g', log=True)
    dis_bar = plt.bar(ind+width, dis_numflows, width, color='b', log=True)
    plt.legend((pdis_bar[0], dis_bar[0]), ('Pre-Disaggregation', 'Disaggregated'))
    plt.savefig('graph5_trafficvolume.pdf')
    print 'graph5'
    return


# Graph 6: Spatial traffic distribution in \dis and \pdis. A n \times n matrix heat diagram, with cell $(i, j)$ having heat level corresponding to the traffic volume between source i and destination j (aggregated across time). The same thing for \pdis.
def graph6(pdis, dis):
    def makeGraph(name, apps, app):
        flows = apps[app]  # confirm whether just one application is ok
        hosts = list(set(f['src'] for f in flows) | set(f['dst'] for f in flows))
        hosts.sort()
        hostmap = {hosts[i]: i for i in xrange(len(hosts))}
        heatmap = np.zeros((len(hosts), len(hosts)))
        sdpairs = sum(([(i, j) for j in hosts if i != j] for i in hosts), [])
        sdflows = {(s, d): sum(f['size'] for f in flows if f['src'] == s and f['dst'] == d) for s, d in sdpairs}
        for s, d in sdflows.keys():
            s1 = hostmap[s]
            d1 = hostmap[d]
            heatmap[s1][d1] = sdflows[(s, d)]

        plt.cla()
        plt.clf()
        plt.ylim(0, 13)
        plt.xlim(0, 13)
        plt.xlabel('Sources')
        plt.ylabel('Destinations')
        plt.pcolor(heatmap)
        plt.savefig(name.format(app))
    for app in pdis.keys():
        makeGraph('graph6_trafficvolumeheatmap_pdis_{}.pdf', pdis, app)
        makeGraph('graph6_trafficvolumeheatmap_dis_{}.pdf', dis, app)
    print 'graph6'
    return


# Graph 7: Temporal traffic distribution in \dis and \pdis for one of the applications. Not sure if there is a good way to fit all the applications in one figure; but if not, we could put one of the results and refer to the appendix for the remaining applications.
def graph7(pdis, dis):
    def makeGraph(name, apps, app):
        flows = apps[app]
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
        plt.savefig(name.format(app))
    for app in pdis.keys():
        makeGraph('graph7_temporaltraffic_pdis_{}.pdf', pdis, app)
        makeGraph('graph7_temporaltraffic_dis_{}.pdf', dis, app)
    print 'graph7'
    return


def collectAllFlows():
    apps = ['graphlab', 'memcached', 'terasort', 'wordcount', 'wordcount_hadoop']  # "don't show anything from storm" - peter
    apps = [(s, 'results/{}'.format(s)) for s in apps]
    pdis_file = '{}/nic_flows.txt'
    dis_file = '{}/res-based_timeonly_flows.txt'
    pdis = {}
    dis = {}
    for a, a_dir in apps:
        pdis[a] = readFlows(pdis_file.format(a_dir))
        dis[a] = readFlows(dis_file.format(a_dir))
    return pdis, dis

if __name__ == '__main__':
    pdis, dis = collectAllFlows()
    assert(len(pdis) == len(dis))
    fns = [graph6, graph7]  # [graph1, graph2, graph3, graph4, graph5]
    map(lambda x: x(pdis, dis), fns)
