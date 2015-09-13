#!/usr/bin/python

import sys
import subprocess
import itertools
import random

# import numpy as np
# import matplotlib
# matplotlib.use('Agg')
# import matplotlib.pyplot as plt

import threading

import pdb

ARCH_RACK_SCALE = 'rack-scale'
ARCH_RES_BASED = 'res-based'

COMB_NONE = 'plain'
COMB_TIMEONLY = 'timeonly'
COMB_ALL = 'combined'


def readMemoryLine(line, node):
    '''
    Read the output of /rmem/results/<blah>/<vm#-mem/disk-ec2id#-partialTrace>
    <record id> <utc timestamp> <page location> <length, pages> <pg size= 4 KB>
    new:
    <record id> <utc timestamp> <page location> <length, pages> <batch seq no>
    when batch seq no == 0, start a new flow, otherwise aggregate.
    '''
    rid, timestamp, addr, length, batch_seq_no = line.split()
    pgSize = 4096

    rw = 'r' if timestamp.startswith('-') else 'w'
    if (rw == 'r'):
        timestamp = int(timestamp) * -1
    else:
        timestamp = int(timestamp)

    timestamp /= 1e6
    rid = int(rid)
    addr = int(addr)  # + (23e9/4096) * node  # remote memory is 23GB per machine
    length = int(length) * int(pgSize)  # get in bytes
    return {
        'rid': rid,
        'time': timestamp,
        'node': node,
        'rw': rw,
        'addr': addr,
        'length': length,
        'batch_seq_no': int(batch_seq_no)
    }


def readMemoryTrace(filename, node):
    '''
    all memory accesses in a given trace are between 2 given nodes.
    '''
    with open(filename, 'r') as trace:
        return [readMemoryLine(line, node) for line in trace]


def readDiskLine(line, time_offset, node):
    try:
        _, timestamp, _, addr, _, length, _, _, _, rw, _, _ = line.split()
    except ValueError:
        return None
    timestamp = time_offset + float(timestamp)
    addr = int(addr)  # + (77e9/4096) * node  # 77 GB of disk per machine
    length = int(length) * 4096  # disk block size = 4KB
    rw = rw[0].lower()
    return {
        'time': timestamp,
        'node': node,
        'rw': rw,
        'addr': addr,
        'length': length
    }


def readDiskTrace(diskFilename, tsFilename, node):
    '''
    Disk trace times are offsets in seconds from start of run
    So read offset and add to convert to epoch time.
    '''
    f = open(tsFilename, 'r')
    offset = float(f.readline()) / 1e6
    f.close()
    out = subprocess.check_output("blkparse {0} | egrep -v 'python|tcpdump|blktrace|cat|swap|bash|sh|auditd' | python get_disk_io.py".format(diskFilename), shell=True)
    out = out.split("\n")
    return list(itertools.ifilter((lambda x: x is not None), (readDiskLine(line, offset, node) for line in out)))


def readNicFlows(fname, node):
    def stripPort(name):
        return '.'.join(name.split('.')[:-1])

    with open(fname) as f:
        return map(lambda l: {'node': node, 'start_time': float(l[0]), 'end_time': float(l[1]), 'src': stripPort(l[2]), 'dst': stripPort(l[3]), 'size': int(l[4])}, (l.split() for l in f.readlines()))


def readNicMap(fname):
    with open(fname) as f:
        nm = {sp[-1]: int(sp[0]) for sp in (l.split() for l in f.readlines())}
        print nm
        return nm


def readFiles(fileNames):
    fns = {}
    for name in fileNames:
        fname = name.split('/')[-1]
        if fname == 'traceinfo.txt':
            continue
        if 'addr_mapping.txt' == fname:
            fns['nicmap'] = name
            continue
        node = int(fname.split('-')[0])
        if node not in fns:
            fns[node] = {}
        if '-mem-' in fname:
            fns[node]['mem'] = name
        elif '-disk-' in fname:
            if (name[-1] != '0'):
                continue
            else:
                fns[node]['disk'] = name
        elif '-meta-' in fname:
            fns[node]['meta'] = name
        elif '-nic-' in fname:
            fns[node]['nic'] = name
        else:
            assert(False)

    def readNode(node, n, lock):
        mem = readMemoryTrace(n['mem'], node)
        disk = readDiskTrace(n['disk'], n['meta'], node)
        nic = readNicFlows(n['nic'], node)
        lock.acquire()
        nodes[node] = {'mem': mem, 'disk': disk, 'nic': nic, 'lock': threading.Lock()}
        lock.release()
        return

    nodes = {}
    nodes['nicmap'] = readNicMap(fns['nicmap'])
    lock = threading.Lock()
    threads = [threading.Thread(target=readNode, args=(n, fns[node], lock)) for n in fns.keys() if n != 'nicmap']
    [t.start() for t in threads]
    [t.join() for t in threads]

    print 'readFiles'
    return nodes


def getTrafficData(nodes):
    nodes_keys = filter(lambda f: f != 'nicmap', nodes.keys())
    times = [f['time'] for f in itertools.chain(
        itertools.chain.from_iterable(nodes[n]['disk'] for n in nodes_keys),
        itertools.chain.from_iterable(nodes[n]['mem'] for n in nodes_keys)
    )]
    earliestTime = min(times)
    duration = max(times) - earliestTime

    memAddrs = [m['addr'] for m in itertools.chain.from_iterable(nodes[n]['mem'] for n in nodes_keys)]
    memRange = max(memAddrs) - min(memAddrs)
    diskAddrs = [d['addr'] for d in itertools.chain.from_iterable(nodes[n]['disk'] for n in nodes_keys)]
    diskRange = max(diskAddrs) - min(diskAddrs)

    memTotalVolume = sum(m['length'] for m in itertools.chain.from_iterable(nodes[n]['mem'] for n in nodes_keys))
    diskTotalVolume = sum(d['length'] for d in itertools.chain.from_iterable(nodes[n]['disk'] for n in nodes_keys))

    memBandiwdthDemandPerUnit, diskBandwidthDemandPerUnit = ((memTotalVolume * 8) / duration) / (memRange * 4096 / 1e9), ((diskTotalVolume * 8) / duration) / (diskRange * 4096 / 1e9)

    print 'ranges (page addressed)', memRange, diskRange
    print 'duration (s)', duration
    print 'volumes (bytes)', memTotalVolume, diskTotalVolume
    print 'bandwidth per unit resource (bps / 1 GB memory, bps / 1 GB disk)', memBandiwdthDemandPerUnit, diskBandwidthDemandPerUnit

    return (earliestTime, duration, memRange, memTotalVolume, memBandiwdthDemandPerUnit, diskRange, diskTotalVolume, diskBandwidthDemandPerUnit)


def makeFlows(nodes, data, opts):
    random.seed(0)  # opts[0] = {res-based, rack-scale}, opts[1] for collapseFlows
    numNodes = len(nodes)

    if (opts[0] == ARCH_RES_BASED):
        hosts = range(numNodes * 2 + 3)  # numNodes cpus, numNodes memory, 3 disk
    elif (opts[0] == ARCH_RACK_SCALE):
        hosts = range(numNodes)
    else:
        assert(False)

    earliestTime, duration, memRange, memTotalVolume, memBandiwdthDemandPerUnit, diskRange, diskTotalVolume, diskBandwidthDemandPerUnit = data
    nicmap = nodes['nicmap']
    del nodes['nicmap']

    flowsByNode = {}

    def processNode(n, nodes):
        assert('nicmap' not in nodes.keys())
        nodes[n]['lock'].acquire()
        flowsByNode[n]['mem'] = []
        flowsByNode[n]['disk'] = []

        def processMemFlows():
            mems = nodes[n]['mem']
            memFlows = []
            if (len(mems) > 0):
                memAddrs = [m['addr'] for m in mems]
                localRange = max(memAddrs) - min(memAddrs)
                for mem in mems:
                    h = int((mem['addr'] / localRange) * numNodes)
                    if (opts[0] == ARCH_RES_BASED):
                        h += numNodes  # there are as many memory nodes as CPU nodes.

                    if (mem['rw'] == 'r'):
                        src = hosts[h if h < len(hosts) else (len(hosts) - 1)]
                        dst = hosts[n]
                        typ = "memRead"
                    else:
                        src = hosts[n]
                        dst = hosts[h if h < len(hosts) else (len(hosts) - 1)]
                        typ = "memWr"
                    if (src == dst):
                        assert (opts[0] == ARCH_RACK_SCALE), "mem {} {} {} {} {} {}".format(opts[0], h, n, numNodes, src, dst)
                        continue

                    memFlows.append(
                        {
                            'time': mem['time'] - earliestTime,
                            'src': src,
                            'dst': dst,
                            'size': mem['length'],
                            'type': typ,
                            'disp-addr': str(n) + '-' + str(mem['addr']),
                            'addr': mem['addr']
                        }
                    )
                flowsByNode[n]['disk'] += memFlows
                del memAddrs
                del memFlows

        def processDiskFlows():
            disks = nodes[n]['disk']
            nicFlows = [{'start_time': f['start_time'], 'end_time': f['end_time'], 'size': f['size'], 'src': nicmap[f['src']], 'dst': nicmap[f['dst']]} for f in nodes[n]['nic'] if nicmap[f['src']] != -1 and nicmap[f['dst']] != -1]
            nicFlows = filter(lambda f: f['dst'] == n, nicFlows)
            if (len(nicFlows) == 0):
                pdb.set_trace()
            nicFlows.sort(key=lambda f: -1 * f['start_time'])

            disks.sort(key=lambda f: f['time'])
            diskFlows = []

            if (len(disks) > 0):
                diskAddrs = [d['addr'] for d in disks]
                localRange = max(diskAddrs) - min(diskAddrs)
                currNicFlow = nicFlows.pop()
                for disk in disks:
                    if (opts[0] == ARCH_RES_BASED):
                        h = int((disk['addr'] / localRange) * 3) + (2 * numNodes)  # there are 3 disk nodes.
                    else:
                        h = int((disk['addr'] / localRange) * numNodes)

                    if (disk['rw'] == 'w'):
                        src = hosts[n]
                        dst = hosts[h if h < len(hosts) else (len(hosts) - 1)]
                        typ = "diskWr"
                    else:
                        time = disk['time']
                        if (currNicFlow is not None and 'size' not in currNicFlow.keys()):
                            pdb.set_trace()
                        while (currNicFlow is not None and not (currNicFlow['size'] > disk['length'] and time > currNicFlow['start_time'] and time < currNicFlow['end_time'])):
                            currNicFlow = nicFlows.pop() if len(nicFlows) > 0 else None

                        if (currNicFlow is not None):
                            currNicFlow['size'] -= disk['length']

                            src = hosts[h if h < len(hosts) else (len(hosts) - 1)]
                            # assign this disk flow to the source of the nic flow
                            dst = hosts[currNicFlow['src']]
                        else:
                            src = hosts[h if h < len(hosts) else (len(hosts) - 1)]
                            dst = hosts[n]

                        typ = "diskRead"
                    if (src == dst):
                        assert (opts[0] == ARCH_RACK_SCALE), "disk {} {} {} {}".format(opts[0], h, n, numNodes)
                        continue

                    diskFlows.append(
                        {
                            'time': disk['time'] - earliestTime,
                            'src': src,
                            'dst': dst,
                            'size': disk['length'],
                            'type': typ,
                            'disp-addr': str(n) + '-' + str(disk['addr']),
                            'addr': disk['addr']
                        }
                    )
                flowsByNode[n]['disk'] += diskFlows
                del diskAddrs
                del diskFlows

        processThreads = [threading.Thread(target=processMemFlows), threading.Thread(target=processDiskFlows)]
        [t.start() for t in processThreads]
        [t.join() for t in processThreads]

        print n, len(flowsByNode[n]['mem']) + len(flowsByNode[n]['disk'])
        nodes[n]['lock'].release()
        del nodes[n]

    threads = [threading.Thread(target=processNode, args=(n, nodes)) for n in nodes.keys() if n != 'nicmap']
    [t.start() for t in threads]
    [t.join() for t in threads]

    # for n in nodes.keys():
    #    if (n == 'nicmap'):
    #        continue
    #    processNode(n, nodes)

    memFlows = sum((v['mem'] for v in flowsByNode.values()), [])
    diskFlows = sum((v['disk'] for v in flowsByNode.values()), [])

    memFlows.sort(key=lambda f: f['time'])
    diskFlows.sort(key=lambda f: f['time'])
    return memFlows, diskFlows


def collapseFlows(flows, opts):
    def combine(fs):
        first = fs[0]
        time = first['time']
        src = first['src']
        dst = first['dst']
        typ = first['type']
        addr = first['addr']
        dispaddr = first['disp-addr']
        totalSize = sum(f['size'] for f in fs)
        return {'time': time, 'src': src, 'dst': dst, 'type': typ, 'size': totalSize, 'addr': addr, 'disp-addr': dispaddr}

    # fs are flows with same src and dest
    # should group sets of flows that are consecutive and separated by no more than delta = 10 us
    def grouper(fs):
        # dictionary of flow groups. key = (next expected address, time horizon), value = flows in group so far
        groups = {}
        for f in fs:
            found = None
            for addr, horizon in groups.keys():
                if (f['addr'] == addr and f['time'] <= horizon):
                    found = (addr, horizon)
                    break
                elif (f['time'] > horizon):
                    # because flows are sorted by time, if we pass the horizon we yield the group.
                    yield groups[(addr, horizon)]
                    del groups[(addr, horizon)]
            if (found is not None):
                grp = groups[found]
                del groups[found]
                grp.append(f)
                groups[(f['addr'] + f['size'] / 4096, f['time'] + 50e-6)] = grp
            else:
                groups[(f['addr'] + f['size'] / 4096, f['time'] + 50e-6)] = [f]
        # yield remaining groups
        for grp in groups.values():
            yield grp

    def grouper_timeOnly(fs):
        groups = {}
        for f in fs:
            found = None
            for horizon in groups.keys():
                if (f['time'] <= horizon):
                    found = horizon
                    break
                else:
                    yield groups[horizon]
                    del groups[horizon]
            if (found is not None):
                grp = groups[found]
                del groups[found]
                grp.append(f)
                groups[f['time'] + 50e-6] = grp
            else:
                groups[f['time'] + 50e-6] = [f]
        # yield remaining groups
        for grp in groups.values():
            yield grp

    if (opts == COMB_NONE):
        return flows
    grp = None
    if (opts == COMB_TIMEONLY):
        grp = grouper_timeOnly
    elif (opts == COMB_ALL):
        grp = grouper
    assert(grp is not None)

    hosts = set(f['src'] for f in flows) | set(f['dst'] for f in flows)
    sdpairs = sum(([(i, j) for j in hosts if i != j] for i in hosts), [])
    sdflows = {(s, d): [f for f in flows if f['src'] == s and f['dst'] == d] for s, d in sdpairs}
    collapsed = []
    for sd in sdflows.keys():
        fs = sdflows[sd]
        fs.sort(key=lambda f: f['time'])
        if (len(fs) > 0):
            collapsed += list(map(combine, grp(fs)))
    collapsed.sort(key=lambda f: f['time'])
    return collapsed


'''
def plotAddressAccessOverTime(flows, prefix=None):
    memFlows = [f for f in flows if 'mem' in f['type']]
    memFlows.sort(key=lambda x: x['time'])
    times = [f['time'] for f in memFlows]
    addrs = [f['addr'] for f in memFlows]

    plt.clf()
    plt.cla()
    plt.title('Addresses Accessed Over Time')
    plt.xlabel('Time')
    plt.ylabel('Address')
    plt.plot(times, addrs, 'b.')
    plt.savefig('address_accesses.png')
'''


def writeFlows(flows, outDir, arrangement, opt):
    print 'writing:', "{0}{1}_{2}_flows.txt".format(outDir, arrangement, opt), ': ', len(flows), ' flows'
    fid = 0
    with open("{0}{1}_{2}_flows.txt".format(outDir, arrangement, opt), 'w') as of:
        for f in flows:
            of.write("{0} {1} {2} {3} {4} {5} {6}\n".format(fid, "%.9f" % f['time'], f['src'], f['dst'], f['size'], f['type'], f['disp-addr']))
            fid += 1


def mergeSortedLists(a, b):
    i = 0
    j = 0
    while (i < len(a) and j < len(b)):
        if (a[i]['time'] < b[j]['time']):
            yield a[i]
            i += 1
        else:
            yield b[j]
            j += 1
    if (i == len(a)):
        while (j < len(b)):
            yield b[j]
            j += 1
    else:
        while (i < len(a)):
            yield a[i]
            i += 1


def run(outDir, traces):
    nodes = readFiles(traces)
    data = getTrafficData(nodes)
    print data
    for arrangement in [ARCH_RES_BASED]:
        mem, disk = makeFlows(nodes, data, (arrangement, COMB_NONE))
        writeFlows(mergeSortedLists(mem, disk), outDir, arrangement, COMB_NONE)

        for opt in [COMB_TIMEONLY]:
            # only disk flows get combined.
            disk_col_flows = collapseFlows(disk, opt)
            writeFlows(mergeSortedLists(mem, disk_col_flows), outDir, arrangement, opt)


if __name__ == '__main__':
    if (len(sys.argv) < 2):
        print 'Usage: python makeFlowTrace.py <outfile> <IO traces...>'
        sys.exit(1)
    run(sys.argv[1], sys.argv[2:])
