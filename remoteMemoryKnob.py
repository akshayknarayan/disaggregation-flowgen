from makeFlowTrace import run

import sys
import subprocess


traceinfoTemplate = '{}/traceinfo.txt'


def readTraceInfo(fileName):
    with open(fileName, 'r') as f:
        rmemgb = float(f.readlines()[-1].split()[6])
    return (rmemgb / 29.45) * 100  # return percentage of remote memory


if __name__ == '__main__':
    if (len(sys.argv) < 2):
        print 'Usage: python remoteMemoryKnob.py <outdir> <remote memory dirs...>'
    outDir = sys.argv[1]
    for rmemdir in sys.argv[2:]:
        percentage = readTraceInfo(traceinfoTemplate.format(rmemdir))
        subprocess.call('mkdir -p {}/{}_remote'.format(outDir, percentage), shell=True)
        traces = map((lambda y: '{}/{}'.format(rmemdir, y)),
                     filter((lambda x: x != 'traceinfo.txt'),
                            subprocess.check_output("ls {}".format(rmemdir), shell=True).split()))
        run('{}/{}_remote/'.format(outDir, percentage), traces)
