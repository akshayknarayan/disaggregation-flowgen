from makeFlowTrace import run

import sys
import subprocess
import threading

traceinfoTemplate = '{}/traceinfo.txt'


def readTraceInfo(fileName):
    with open(fileName, 'r') as f:
        rmemgb = float(f.readlines()[-1].split()[6])
    return (rmemgb / 29.45) * 100  # return percentage of remote memory


def runexp(outDir, percentage, traces):
    run('{}/{}_remote/'.format(outDir, percentage), traces)
    print "finished", rmemdir

if __name__ == '__main__':
    if (len(sys.argv) < 2):
        print 'Usage: python remoteMemoryKnob.py <outdir> <remote memory dirs...>'
    outDir = sys.argv[1]
    threads = []
    for rmemdir in sys.argv[2:]:
        percentage = readTraceInfo(traceinfoTemplate.format(rmemdir))
        print rmemdir, percentage
        subprocess.call('mkdir -p {}/{}_remote'.format(outDir, percentage), shell=True)
        traces = map((lambda y: '{}/{}'.format(rmemdir, y)),
                     filter((lambda x: x != 'traceinfo.txt'),
                            subprocess.check_output("ls {}".format(rmemdir), shell=True).split()))
        threads.append((percentage, threading.Thread(target=runexp, args=(outDir, percentage, traces))))

    threads.sort(key=lambda a: a[0])
    for t in threads:
        t[1].start()
        t[1].join(3600)
