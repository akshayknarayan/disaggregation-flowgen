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

# import pdb

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
    def readMemoryTrace_gen(lines):
        curr = None
        for line in lines:
            memFlow = readMemoryLine(line, node)
            if (curr is None):
                curr = memFlow
                continue
            elif (memFlow['batch_seq_no'] == 0):
                yield curr
                curr = memFlow
            else:
                curr['length'] += memFlow['length']
        yield curr

    with open(filename, 'r') as trace:
        # return [readMemoryLine(line, node) for line in trace]
        return list(readMemoryTrace_gen(trace))


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


def readFiles(fileNames):
    fns = {}
    for name in fileNames:
        print name
        fname = name.split('/')[-1]
        if fname == 'traceinfo.txt':
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
            fns[node]['ts'] = name
        elif '-nic-' in fname:
            continue  # nic trace should be separate.
        else:
            assert(False)

    def readNode(node, n, outNodes, lock):
        mem = readMemoryTrace(n['mem'], node)
        disk = readDiskTrace(n['disk'], n['ts'], node)
        lock.acquire()
        print node
        nodes[node] = {'mem': mem, 'disk': disk, 'lock': threading.Lock()}
        lock.release()
        return

    nodes = {}
    lock = threading.Lock()
    threads = [threading.Thread(target=readNode, args=(n, fns[node], nodes, lock)) for n in fns.keys()]
    [t.start() for t in threads]
    [t.join() for t in threads]

    print 'readFiles'
    return nodes


def getTrafficData(nodes):
    times = [f['time'] for f in itertools.chain(
        itertools.chain.from_iterable(nodes[n]['disk'] for n in nodes.keys()),
        itertools.chain.from_iterable(nodes[n]['mem'] for n in nodes.keys())
    )]
    earliestTime = min(times)
    duration = max(times) - earliestTime

    memAddrs = [m['addr'] for m in itertools.chain.from_iterable(nodes[n]['mem'] for n in nodes.keys())]
    memRange = max(memAddrs) - min(memAddrs)
    diskAddrs = [d['addr'] for d in itertools.chain.from_iterable(nodes[n]['disk'] for n in nodes.keys())]
    diskRange = max(diskAddrs) - min(diskAddrs)

    memTotalVolume = sum(m['length'] for m in itertools.chain.from_iterable(nodes[n]['mem'] for n in nodes.keys()))
    diskTotalVolume = sum(d['length'] for d in itertools.chain.from_iterable(nodes[n]['disk'] for n in nodes.keys()))

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

    flowsByNode = {}

    def processNode(n, nodes):
        nodes[n]['lock'].acquire()
        flowsByNode[n] = []

        mems = nodes[n]['mem']
        memFlows = []
        if (len(mems) > 0):
            memAddrs = [m['addr'] - ((23e9/4096) * m['node']) for m in mems]
            localRange = max(memAddrs) - min(memAddrs)
            for mem in mems:
                # local_addr = m['addr'] - (23e9/4096) * mem['node']
                h = int((m['addr'] / localRange) * numNodes)
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
            flowsByNode[n] += collapseFlows(memFlows, opts[1])
            del memAddrs
            del memFlows

        disks = nodes[n]['disk']
        diskFlows = []
        if (len(disks) > 0):
            diskAddrs = [d['addr'] - (77e9/4096) * d['node'] for d in disks]
            localRange = max(diskAddrs) - min(diskAddrs)
            for disk in disks:
                #  local_addr = d['addr'] - (77e9/4096) * d['node']
                if (opts[0] == ARCH_RES_BASED):
                    h = int((d['addr'] / localRange) * 3) + (2 * numNodes)  # there are 3 disk nodes.
                else:
                    h = int((d['addr'] / localRange) * numNodes)

                if (disk['rw'] == 'r'):
                    src = hosts[h if h < len(hosts) else (len(hosts) - 1)]
                    dst = hosts[n]
                    typ = "diskRead"
                else:
                    src = hosts[n]
                    dst = hosts[h if h < len(hosts) else (len(hosts) - 1)]
                    typ = "diskWr"
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
            flowsByNode[n] += collapseFlows(diskFlows, opts[1])
            del diskAddrs
            del diskFlows

        print n, len(flowsByNode[n])
        nodes[n]['lock'].release()
        del nodes[n]

    threads = [threading.Thread(target=processNode, args=(n, nodes)) for n in nodes.keys()]
    [t.start() for t in threads]
    [t.join() for t in threads]

    flows = sum(flowsByNode.values(), [])
    flows.sort(key=lambda f: f['time'])
    return flows


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
    fid = 0
    with open("{0}{1}_{2}_flows.txt".format(outDir, arrangement, opt), 'w') as of:
        for f in flows:
            of.write("{0} {1} {2} {3} {4} {5} {6}\n".format(fid, "%.9f" % f['time'], f['src'], f['dst'], f['size'], f['type'], f['disp-addr']))
            fid += 1


def run(outDir, traces):
    nodes = readFiles(traces)
    data = getTrafficData(nodes)
    print data
    for arrangement in [ARCH_RES_BASED]:
        flows = makeFlows(nodes, data, (arrangement, COMB_NONE))
        writeFlows(flows, outDir, arrangement, COMB_NONE)

        for opt in [COMB_TIMEONLY]:
            print "{0}{1}_{2}_flows.txt".format(outDir, arrangement, opt)
            col_flows = collapseFlows(flows, opt)
            print len(col_flows)
            #  plotAddressAccessOverTime(flows)
            writeFlows(col_flows, outDir, arrangement, opt)


if __name__ == '__main__':
    if (len(sys.argv) < 2):
        print 'Usage: python makeFlowTrace.py <outfile> <IO traces...>'
        sys.exit(1)
    run(sys.argv[1], sys.argv[2:])
