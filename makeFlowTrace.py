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

def getFlowInfo(nodes):
    onlyMem = list(itertools.chain.from_iterable(nodes[i]['mem'] for i in nodes))
    onlyDisk = list(itertools.chain.from_iterable(nodes[i]['disk'] for i in nodes))
    allFlows = list(itertools.chain.from_iterable(nodes[i]['mem'] + nodes[i]['disk'] for i in nodes))

    earliestTime = min(n['time'] for n in allFlows)
    lastTime = max(n['time'] for n in allFlows)

    memAddressRange = [n['addr'] for n in onlyMem]
    diskAddressRange = [n['addr'] for n in onlyDisk]
    lengths = [n['length'] for n in allFlows]

    smallestMemAddress = min(memAddressRange)
    largestMemAddress = max(memAddressRange)
    memoryRange = largestMemAddress - smallestMemAddress

    smallestDiskAddress = min(diskAddressRange)
    largestDiskAddress = max(diskAddressRange)
    diskRange = largestDiskAddress - smallestDiskAddress

    print 'flows', np.mean(lengths), len(lengths)
    print 'time', earliestTime, lastTime, lastTime - earliestTime
    print 'mem', smallestMemAddress, largestMemAddress, memoryRange
    print 'disk', smallestDiskAddress, largestDiskAddress, diskRange

    return memoryRange, onlyMem, diskRange, onlyDisk, earliestTime

def makeFlows(nodes):
    memoryRange, onlyMem, diskRange, onlyDisk, earliestTime = getFlowInfo(nodes)
    hosts = random.sample(xrange(144), len(nodes))

    def whichMem(addr):
        return int(addr/(memoryRange/len(nodes))) % 10
    def whichDisk(addr):
        return int(addr/(diskRange/len(nodes))) % 10

    flows = []
    for mem in onlyMem:
        if (mem['rw'] == 'r'):
            src = hosts[whichMem(mem['addr'])]
            dst = hosts[mem['node']]
            typ = "memRead"
        else:
            src = hosts[mem['node']]
            dst = hosts[whichMem(mem['addr'])]
            typ = "memWr"
        if (src == dst):
            continue
        flows.append({'time':mem['time'] - earliestTime, 'src':src, 'dst':dst, 'size':mem['length'], 'type':typ})
    for disk in onlyDisk:
        if (disk['rw'] == 'r'):
            src = hosts[whichDisk(disk['addr'])]
            dst = hosts[disk['node']]
            typ = "diskRead"
        else:
            src = hosts[disk['node']]
            dst = hosts[whichDisk(disk['addr'])]
            typ = "diskWr"
        if (src == dst):
            continue
        flows.append({'time':disk['time'] - earliestTime, 'src':src, 'dst':dst, 'size':disk['length'], 'type':typ})
    return flows

# if two flows have the same source and destination and start within epsilon of each other, combine them.
def collapseFlows(flows):
    epsilon = 5e-6
    def combine(fs):
        first = next(fs)
        time = first['time']
        src = first['src']
        dst = first['dst']
        typ = first['type']
        totalSize = first['size'] + sum(f['size'] for f in fs)
        return {'time':time, 'src':src, 'dst':dst, 'type':typ, 'size':totalSize}

    keyfunc = lambda f:",".join(map(str,(f['type'], f['src'], f['dst'], int(f['time']))))
    flows.sort(key = keyfunc)
    collapsed = []
    for k, g in itertools.groupby(flows, keyfunc):
        collapsed.append(combine(g))
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
            of.write("{0} {1} {2} {3} {4} {5}\n".format(fid, "%.9f" % f['time'], f['src'], f['dst'], f['size'], f['type']))
            fid += 1
    print uncollapsed_len, fid
    print 'collapsed', uncollapsed_len - fid, 'flows'
