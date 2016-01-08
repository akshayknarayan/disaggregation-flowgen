#!/usr/bin/python

import subprocess
import numpy as np
# import scipy.stats
import matplotlib
import matplotlib.cm
import matplotlib.colors
import itertools
# from scipy.interpolate import UnivariateSpline
matplotlib.use('Agg')
from matplotlib import pyplot as plt

from readFlowTrace import cdf, interarrivals
import threading

import pickle
import pdb

# Graph function takes 2 dicts with structure keys = application name, value = list of flows

keys_sorted = ['wordcount-hadoop', 'wordcount-spark', 'terasort-hadoop', 'terasort-spark', 'graphlab', 'memcached', 'bdb']

# Graph 1: Flow size distribution in \dis and \pdis. All applications go into one figure: multiple lines in the CDF, one for each application. The same thing for \pdis.
def graph1(pdis, dis):
    def makeGraph(name, apps):
        plt.cla()
        plt.clf()
        plt.ylim(0, 1)
        plt.xlim(1, 1e10)
        plt.ylabel('CDF')
        plt.xlabel('Flow Size, Bytes')
        for app in keys_sorted:
            x, y = cdf([f['size'] for f in apps[app]])
            y[-1] = 1.0
            plt.semilogx(x, y, label=app)
        if ('_pdis' in name):
            plt.legend(loc='lower right')
        else:
            plt.legend(loc='lower right')
        plt.savefig(name)
    makeGraph('graph1_sizedist_pdis.pdf', pdis)
    makeGraph('graph1_sizedist_dis.pdf', dis)
    print 'graph1'
    return


# Graph 2: Number of flows in \dis and \pdis. One single bar graph, two bars for each application --- one for #flows in \dis and the other for #flows in \pdis.
def graph2(pdis, dis):
    pdis_numflows = [len(pdis[k]) for k in keys_sorted]
    dis_numflows = [len(dis[k]) for k in keys_sorted]

    plt.cla()
    plt.clf()
    ind = np.arange(len(pdis))
    width = 0.35
    plt.xlabel('Application')
    plt.ylabel('Number of Flows')
    plt.ylim(1e1, 1e9)
    plt.xticks(ind + width, tuple(keys_sorted))
    pdis_bar = plt.bar(ind, pdis_numflows, width, log=True, color='coral')
    dis_bar = plt.bar(ind+width, dis_numflows, width, log=True, color='lightblue')
    plt.legend((pdis_bar[0], dis_bar[0]), ('Server Centric', 'Disaggregated'), bbox_to_anchor=(0., 1.02, 1., .102), loc=3, ncol=2, mode="expand", borderaxespad=0.)
    plt.savefig('graph2_numflows.pdf')
    print 'graph2'
    return


# Graph 4: Flow arrival time distribution in \dis and \pdis. All the applications go into one figure: multiple lines in the CDF, one for each application. The same thing for \pdis.
def graph4(pdis, dis):
    #return
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
    pdis_bytes = [sum(f['size'] for f in pdis[k]) / 1e9 for k in keys_sorted]
    dis_bytes = [sum(f['size'] for f in dis[k]) / 1e9 for k in keys_sorted]
    # pdb.set_trace()

    plt.cla()
    plt.clf()
    ind = np.arange(len(pdis))
    width = 0.35
    plt.xlabel('Application')
    plt.ylabel('Traffic Volume (GB)')
    plt.ylim(1, 1e5)
    plt.xticks(ind + width, tuple(keys_sorted))
    pdis_bar = plt.bar(ind, pdis_bytes, width, color='coral', log=True)
    dis_bar = plt.bar(ind+width, dis_bytes, width, color='lightblue', log=True)
    plt.legend((pdis_bar[0], dis_bar[0]), ('Server Centric', 'Disaggregated'), bbox_to_anchor=(0., 1.02, 1., .102), loc=3, ncol=2, mode="expand", borderaxespad=0.)
    plt.savefig('graph5_trafficvolume.pdf')
    print 'graph5'
    return


# Graph 6: Spatial traffic distribution in \dis and \pdis. A n \times n matrix heat diagram, with cell $(i, j)$ having heat level corresponding to the traffic volume between source i and destination j (aggregated across time). The same thing for \pdis.
def graph6(pdis, dis):
    return

    ## we're not doing this.


    def makeMaps(apps, app):
        flows = apps[app]
        hosts = list(set(int(f['src']) for f in flows) & set(int(f['dst']) for f in flows))
        hosts.sort()
        heatmap = [[0 for _ in range(len(hosts))] for _ in range(len(hosts))]
        sdpairs = sum(([(i, j) for j in hosts if i != j] for i in hosts), [])
        sdflows = {(s, d): sum(f['size'] for f in flows if int(f['src']) == s and int(f['dst']) == d) for s, d in sdpairs}
        for s, d in sdflows.keys():
            if (s not in hosts or d not in hosts or s >= len(heatmap) or d >= len(heatmap)):
                continue
            heatmap[s][d] = sdflows[(s, d)]

        # eliminate extra row
        heatmap = map(lambda l: l[:5] + l[6:], heatmap)
        heatmap = heatmap[:5] + heatmap[6:]
        heatmap = np.array(heatmap)
        return heatmap

    def makeGraph(name, app, heatmap):
        plt.cla()
        plt.clf()
        plt.ylim(0, len(heatmap))
        plt.xlim(0, len(heatmap))
        plt.xlabel('Sources')
        plt.ylabel('Destinations')
        if ("_pdis" in name):
            plt.xticks(np.arange(5) + 0.5, np.arange(5))
            plt.yticks(np.arange(5) + 0.5, np.arange(5))
        else:
            plt.xticks(np.arange(13) + 0.5, (['C'] * 5 + ['M'] * 5 + ['D'] * 3))
            plt.yticks(np.arange(13) + 0.5, (['C'] * 5 + ['M'] * 5 + ['D'] * 3))
        norm = matplotlib.colors.LogNorm(vmin=1e0, vmax=1e12)
        hmap = plt.pcolor(heatmap, cmap=matplotlib.cm.Reds, norm=norm)
        cbar = plt.colorbar(hmap)
        # cbar.ax.set_yticklabels(np.logspace(0, 12, num=13))
        cbar.ax.set_yticklabels(['$10^{%d}$' % i for i in range(13)])
        plt.savefig(name.format(app))

    try:
        heatmaps = pickle.load(open('heatmaps.pickle', 'r'))
    except:
        heatmaps = {}
        for app in pdis.keys():
            heatmaps[app] = {}
            m1 = makeMaps(dis, app)
            heatmaps[app]['dis'] = m1
            m2 = makeMaps(pdis, app)
            heatmaps[app]['pdis'] = m2

        pickle.dump(heatmaps, open('heatmaps.pickle', 'w'))

    for app in pdis.keys():
        makeGraph('graph6_trafficvolumeheatmap_pdis_{}.pdf', app, heatmaps[app]['pdis'])
        makeGraph('graph6_trafficvolumeheatmap_dis_{}.pdf', app, heatmaps[app]['dis'])
    print 'graph6'
    return


# Graph 7: Temporal traffic distribution in \dis and \pdis for one of the applications. Not sure if there is a good way to fit all the applications in one figure; but if not, we could put one of the results and refer to the appendix for the remaining applications.
def graph7(pdis, dis):
    def makeGraph(name, apps, app):
        flows = apps[app]
        flows.sort(key=lambda x: x['time'])
        slotDuration = 0.0001  # 100 ms= 0.1 s slots

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
        plt.ylabel('Bytes per Second')
        plt.ylim(1, 1e10)
        plt.semilogy(xaxis, yaxis, 'b.')
        plt.savefig(name.format(app))
    for app in pdis.keys():
        makeGraph('graph7_temporaltraffic_pdis_{}.pdf', pdis, app)
        makeGraph('graph7_temporaltraffic_dis_{}.pdf', dis, app)
    print 'graph7'
    return


def readFlows(filename):
    return [{
            'time': float(sp[1]),
            'src': sp[2],
            'dst': sp[3],
            'size': int(sp[4]),
            } for sp
            in (l.split() for l in open(filename))
            ]


def collectAllFlows():
    apps = ['bdb', 'graphlab', 'terasort_hadoop', 'terasort_spark', 'wordcount_hadoop', 'wordcount_spark']
    apps = [(s, 'flowgen-spark/results/{}'.format(s)) for s in apps]
    pdis_file = '{}/nic/part-00000'
    dis_file = '{}/flows/part-00000'

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
    print 'read flows'
    assert(len(pdis) == len(dis))
    fns = [graph1, graph2, graph4, graph5, graph7]

    map(lambda x: x(pdis, dis), fns)
    # threads = [threading.Thread(target=f, args=(pdis, dis)) for f in fns]
    # [t.start() for t in threads]
    # [t.join() for t in threads]

    # call me to let me know it's done.
    subprocess.call("curl -X POST https://maker.ifttt.com/trigger/text_me/with/key/cTyEB1Uga6onvmR6HioIs- > /dev/null 2> /dev/null", shell=True)
