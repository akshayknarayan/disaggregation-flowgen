#!/usr/bin/python

import numpy as np
# import scipy.stats
import matplotlib
import itertools
# from scipy.interpolate import UnivariateSpline
matplotlib.use('Agg')
from matplotlib import pyplot as plt

from readFlowTrace import readFlows, cdf, interarrivals
import threading

import pdb

# Graph function takes 2 dicts with structure keys = application name, value = list of flows


# Graph 1: Flow size distribution in \dis and \pdis. All applications go into one figure: multiple lines in the CDF, one for each application. The same thing for \pdis.
def graph1(pdis, dis):
    def makeGraph(name, apps):
        plt.cla()
        plt.clf()
        plt.ylim(0, 1)
        plt.xlim(1, 1e10)
        plt.ylabel('CDF')
        plt.xlabel('Flow Size, Bytes')
        sorted_apps = sorted(apps.keys())
        for app in sorted_apps:
            x, y = cdf([f['size'] for f in apps[app]])
            plt.semilogx(x, y, label=app)
        plt.legend(loc='lower right')
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
    plt.xticks(ind + width, tuple(keys_sorted))
    pdis_bar = plt.bar(ind, pdis_numflows, width, log=True, color='g')
    dis_bar = plt.bar(ind+width, dis_numflows, width, log=True, color='b')
    plt.legend((pdis_bar[0], dis_bar[0]), ('Pre-Disaggregation', 'Disaggregated'), bbox_to_anchor=(0., 1.02, 1., .102), loc=3, ncol=2, mode="expand", borderaxespad=0.)
    plt.savefig('graph2_numflows.pdf')
    print 'graph2'
    return


# Graph 3: #bytes in different flow bucket sizes for \dis and \pdis. One single stack-bar graph, two bars for each application, one stack for each bucket size --- one for #bytes in \dis and the other for #bytes in \pdis.
def graph3(pdis, dis):
    def makeGraph(name, title, apps):
        keys_sorted = sorted(apps.keys())
        buckets = np.logspace(2, 8, num=7)
        bucketized = {k: np.histogram([f['size'] for f in apps[k]], bins=buckets)[0] for k in keys_sorted}

        plt.cla()
        plt.clf()
        plt.xlabel('Bins: Bytes')
        plt.ylabel('Number of Flows')
        plt.ylim(1, 1e7)
        plt.title(title)
        ind = np.arange(len(buckets) - 1)
        plt.xticks(ind, buckets)

        for k in keys_sorted:
            plt.semilogy(ind, bucketized[k], 'o-', label=k)

        if ('_pdis' in name):
            plt.legend(loc='upper left')
        else:
            plt.legend(loc='lower left')
        plt.savefig(name)
    makeGraph('graph3_sizebins_pdis.pdf', 'Pre-Disaggregation Binned Flow Sizes', pdis)
    makeGraph('graph3_sizebins_dis.pdf', 'Post-Disaggregation Binned Flow Sizes', dis)
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
            flowsForSource = lambda s: sorted(filter(lambda f: f['src'] == s, apps[app]), key=lambda f: f['time'])
            perSourceIntersList = [list(interarrivals((f['time'] * 1e6) for f in flowsForSource(s))) for s in srcs]
            inters = sum(perSourceIntersList, [])
            # pdb.set_trace()
            x, y = cdf(inters)
            plt.semilogx(x, y, label=app)
        plt.legend(loc='lower right')
        plt.savefig(name)
    makeGraph('graph4_interdist_pdis.pdf', pdis)
    makeGraph('graph4_interdist_dis.pdf', dis)
    print 'graph4'
    return


# Graph 5: Traffic volume in \dis and \pdis. One single bar graph, two bars for each application --- one for volume in \dis and the other for volume in \pdis.
def graph5(pdis, dis):
    keys_sorted = sorted(pdis.keys())
    pdis_bytes = [sum(f['size'] for f in pdis[k]) for k in keys_sorted]
    dis_bytes = [sum(f['size'] for f in dis[k]) for k in keys_sorted]
    # pdb.set_trace()

    plt.cla()
    plt.clf()
    ind = np.arange(len(pdis))
    width = 0.35
    plt.xlabel('Application')
    plt.ylabel('Traffic Volume (Bytes)')
    plt.ylim(1e10, 1e12)
    plt.xticks(ind + width, tuple(keys_sorted))
    pdis_bar = plt.bar(ind, pdis_bytes, width, color='g', log=False)
    dis_bar = plt.bar(ind+width, dis_bytes, width, color='b', log=False)
    plt.legend((pdis_bar[0], dis_bar[0]), ('Pre-Disaggregation', 'Disaggregated'), loc='upper left')
    plt.savefig('graph5_trafficvolume.pdf')
    print 'graph5'
    return


# Graph 6: Spatial traffic distribution in \dis and \pdis. A n \times n matrix heat diagram, with cell $(i, j)$ having heat level corresponding to the traffic volume between source i and destination j (aggregated across time). The same thing for \pdis.
def graph6(pdis, dis):
    def makeGraph(name, apps, app):
        flows = apps[app]
        hosts = list(set(f['src'] for f in flows) & set(f['dst'] for f in flows))
        hosts.sort()
        hostmap = {hosts[i]: i for i in xrange(len(hosts))}
        heatmap = np.zeros((len(hosts), len(hosts)))
        sdpairs = sum(([(i, j) for j in hosts if i != j] for i in hosts), [])
        sdflows = {(s, d): sum(f['size'] for f in flows if f['src'] == s and f['dst'] == d) for s, d in sdpairs}
        # pdb.set_trace()
        for s, d in sdflows.keys():
            if (s not in hostmap or d not in hostmap):
                continue
            s1 = hostmap[s]
            d1 = hostmap[d]
            heatmap[s1][d1] = sdflows[(s, d)]

        plt.cla()
        plt.clf()
        plt.ylim(0, len(hosts))
        plt.xlim(0, len(hosts))
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
    apps = ['graphlab', 'memcached', 'terasort', 'wordcount', 'wordcount-hadoop']
    apps = [(s, 'results/{}'.format(s)) for s in apps]
    pdis_file = '{}/nic_flows.txt'
    dis_file = '{}/res-based_timeonly_flows.txt'

    pdis = {}
    dis = {}

    def read(a, a_dir):
        pdis[a] = readFlows(pdis_file.format(a_dir))
        dis[a] = readFlows(dis_file.format(a_dir))

    threads = [threading.Thread(target=read, args=a) for a in apps]
    [t.start() for t in threads]
    [t.join() for t in threads]
    return pdis, dis

if __name__ == '__main__':
    pdis, dis = collectAllFlows()
    assert(len(pdis) == len(dis))
    # fns = [graph1, graph2, graph3, graph4, graph5, graph6, graph7]
    fns = [graph2, graph3]
    map(lambda x: x(pdis, dis), fns)
