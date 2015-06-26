#!/usr/bin/python

import sys
import subprocess
import itertools
import random

import numpy as np

'''
Read the output of /rmem/results/<blah>/<vm#-mem/disk-ec2id#-partialTrace>
'''

def readMemoryLine(line, node):
    '''
    <record id> <utc timestamp> <page location> <length in pages> <page size = 4 KB>
    '''
    rid, timestamp, addr, length, pgSize = line.split()
    assert(int(pgSize) == 4096)

    rw = 'r' if timestamp.startswith('-') else 'w'
    if (rw == 'r'):
        timestamp = int(timestamp) * -1
    else:
        timestamp = int(timestamp)

    rid = int(rid)
    addr = int(addr)
    length = int(length) * int(pgSize) # get in bytes
    return { 'rid' : rid, 'time' : timestamp, 'node' : node, 'rw' : rw, 'addr' : addr, 'length' : length }

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
    addr = int(addr)
    length = int(length) * 4096 # disk block size = 4KB
    rw = rw[0].lower()
    return { 'time' : timestamp, 'node' : node, 'rw' : rw, 'addr' : addr, 'length' : length }

def readDiskTrace(diskFilename, tsFilename, node):
    '''
    Disk trace times are offsets in seconds from start of run
    So read offset and add to convert to epoch time.
    '''
    f = open(tsFilename, 'r')
    offset = float(f.read())
    f.close()
    out = subprocess.check_output("blkparse {0} | grep java | python get_disk_io.py".format(diskFilename), shell = True)
    out = out.split("\n")
    return list(itertools.ifilter((lambda x:x is not None), (readDiskLine(line, offset, node) for line in out)))

def readFiles(fileNames):
    fns = {}
    for name in fileNames:
        fname = name.split('/')[-1]
        node = int(fname.split('-')[0])
        if node not in fns:
            fns[node] = {}
        if '-mem-' in fname:
            fns[node]['mem'] = name
        elif '-disk-' in fname:
            if (name[-1] == '1'):
                continue
            else:
                fns[node]['disk'] = name
        elif '-meta-' in fname:
            fns[node]['ts'] = name
        else:
            assert(False)

    nodes = {}
    for node in fns:
        n = fns[node]
        mem = readMemoryTrace(n['mem'], node)
        disk = readDiskTrace(n['disk'], n['ts'], node)
        nodes[node] = { 'mem':mem, 'disk':disk }

    return nodes

def makeFlows(nodes):
    random.seed(0)
    hosts = random.sample(xrange(144), len(nodes))

    def which(node, addrRange, addr):
        ind = range(10)
        random.shuffle(ind)
        return ind[int(addr/(addrRange/len(nodes)))]

    flows = []
    for n in nodes:
        mems = n['mem']
        disks = n['disk']

        memAddrs = [m['addr'] for m in mems]
        memRange = max(memAddrs) - min(memAddrs)

        diskAddrs = [d['addr'] for d in disks]
        diskRange = max(diskAddrs) - min(diskAddrs)

        for mem in mems:
            if (mem['rw'] == 'r'):
                src = hosts[which(n, memRange, mem['addr'])]
                dst = hosts[n]
                typ = "memRead"
            else:
                src = hosts[n]
                dst = hosts[which(n, memRange, mem['addr'])]
                typ = "memWr"
            if (src == dst):
                continue
            flows.append({'time':mem['time'] - earliestTime, 'src':src, 'dst':dst, 'size':mem['length'], 'type':typ, 'addr':mem['addr']})
        for disk in disks:
            if (disk['rw'] == 'r'):
                src = hosts[which(n, diskRange, disk['addr'])]
                dst = hosts[n]
                typ = "diskRead"
            else:
                src = hosts[n]
                dst = hosts[which(n, diskRange, disk['addr'])]
                typ = "diskWr"
            if (src == dst):
                continue
            flows.append({'time':disk['time'] - earliestTime, 'src':src, 'dst':dst, 'size':disk['length'], 'type':typ, 'addr':disk['addr']})
    return flows

# if two flows have the same source and destination and start within epsilon of each other, combine them.
def collapseFlows(flows):
    def combine(fs):
        first = next(fs)
        time = first['time']
        src = first['src']
        dst = first['dst']
        typ = first['type']
        addr = first['addr']
        totalSize = first['size'] + sum(f['size'] for f in fs)
        return {'time':time, 'src':src, 'dst':dst, 'type':typ, 'size':totalSize, 'addr': addr}

    def grouper(fs):
        flows = [fs[0]]
        addr = fs[0]['addr']
        typ = fs[0]['type']
        for f in fs[1:]:
            if (f['type'] != typ or f['addr'] != addr + 1):
                yield flows
                flows.clear()
            else:
                flows.append(f)

    hosts = set(f['src'] for f in flows)
    sdpairs = sum(([(i,j) for j in hosts if i != j] for i in hosts), [])
    sdflows = {(s,d):[f for f in flows if f['src'] == s and f['dst'] == d] for s,d in sdpairs}
    collapsed = []
    for sd in sdflows.keys():
        fs = sdflows[sd]
        fs.sort(key = lambda f:f['time'])
        collapsed += map(combine, grouper(fs))
    collapsed.sort(key = lambda f:f['time'])
    return collapsed

if __name__ == '__main__':
    if (len(sys.argv) < 2):
        print 'Usage: python makeFlowTrace.py <spark IO traces...>'
        sys.exit(1)
    nodes = readFiles(sys.argv[1:])
    flows = makeFlows(nodes)
    uncollapsed_len = len(flows)
    flows = collapseFlows(flows)
    fid = 0
    with open('flows.txt', 'w') as of:
        for f in flows:
            of.write("{0} {1} {2} {3} {4} {5} {6}\n".format(fid, "%.9f" % f['time'], f['src'], f['dst'], f['size'], f['type'], f['addr']))
            fid += 1
    print uncollapsed_len, fid
    print 'collapsed', uncollapsed_len - fid, 'flows'
